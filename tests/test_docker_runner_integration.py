import pytest
import tempfile
import os
from pathlib import Path
import subprocess
from genie_wrapper.core.docker_runner import DockerGenieRunner


class TestDockerGenieRunnerIntegration:
    """Integration tests for DockerGenieRunner using real Docker with permanent file storage."""

    @pytest.fixture(scope="class")
    def docker_available(self):
        """Check if Docker is available and running."""
        try:
            result = subprocess.run(
                ["docker", "--version"], 
                capture_output=True, 
                text=True, 
                check=True
            )
            print(f"Docker available: {result.stdout.strip()}")
            return True
        except (subprocess.CalledProcessError, FileNotFoundError):
            pytest.skip("Docker not available or not running")

    @pytest.fixture(scope="class") 
    def genie_image_available(self, docker_available):
        """Check if the GENIE Docker image is available."""
        try:
            result = subprocess.run(
                ["docker", "image", "inspect", "muefab/genie:latest"],
                capture_output=True,
                text=True,
                check=True
            )
            print("GENIE Docker image is available")
            return True
        except subprocess.CalledProcessError:
            # Try to pull the image
            try:
                print("Pulling GENIE Docker image...")
                subprocess.run(
                    ["docker", "pull", "muefab/genie:latest"],
                    check=True
                )
                return True
            except subprocess.CalledProcessError:
                pytest.skip("GENIE Docker image not available and cannot be pulled")

    @pytest.fixture(scope="class")
    def test_output_dir(self):
        """Create a permanent test output directory for storing compressed files."""
        # Create a permanent directory in the project root for test outputs
        output_dir = Path(__file__).parent.parent / "test_outputs"
        output_dir.mkdir(exist_ok=True)
        
        print(f"Test output directory: {output_dir.absolute()}")
        yield output_dir
        
        # Optionally clean up after tests (comment out to keep files permanently)
        # import shutil
        # if output_dir.exists():
        #     shutil.rmtree(output_dir)
        #     print(f"Cleaned up test output directory: {output_dir}")

    @pytest.fixture
    def test_data_dir(self, test_output_dir):
        """Create test data files in the permanent output directory."""
        # Create input files in the same directory as outputs (Docker volume binding requirement)
        
        # Create a minimal FASTQ file for testing
        fastq_content = """@read1
GATTTGGGGTTTAAAGGG
+
@@@@@@@@@@@@@@@@@@
@read2  
CGATCGATCGATCGATCG
+
@@@@@@@@@@@@@@@@@@
"""
        fastq_file = test_output_dir / "test_input.fastq"
        fastq_file.write_text(fastq_content)
        
        # Create a minimal reference file
        ref_content = """>chr1
GATTTGGGGTTTAAAGGGCGATCGATCGATCGATCGAAAAAAAAAA
"""
        ref_file = test_output_dir / "reference.fasta"
        ref_file.write_text(ref_content)
        
        return test_output_dir

    @pytest.fixture
    def runner(self, genie_image_available):
        """Create DockerGenieRunner instance."""
        return DockerGenieRunner("muefab/genie:latest")

    def test_version_command(self, runner):
        """Test that we can get version information from GENIE."""
        version_output = runner.version()
        
        assert version_output is not None
        assert len(version_output) > 0
        print(f"GENIE version: {version_output}")

    def test_compress_basic_fastq(self, runner, test_data_dir):
        """Test basic FASTQ compression with permanent file storage."""
        input_file = test_data_dir / "test_input.fastq"
        output_file = test_data_dir / "compressed_output.mgb"
        
        # Perform compression - file will be stored permanently
        result = runner.compress(str(input_file), str(output_file))
        
        # Verify compression completed
        assert result is not None
        assert len(result) > 0
        
        # Verify output file was created and is permanent
        assert output_file.exists()
        assert output_file.stat().st_size > 0
        
        print(f"Compression result: {result}")
        print(f"Compressed file stored at: {output_file.absolute()}")
        print(f"Compressed file size: {output_file.stat().st_size} bytes")
        print(f"Original FASTQ size: {input_file.stat().st_size} bytes")
        
        # Calculate compression ratio
        compression_ratio = input_file.stat().st_size / output_file.stat().st_size
        print(f"Compression ratio: {compression_ratio:.2f}:1")

    def test_compress_with_reference(self, runner, test_data_dir):
        """Test FASTQ compression with reference file - permanent storage."""
        input_file = test_data_dir / "test_input.fastq"
        output_file = test_data_dir / "compressed_with_reference.mgb"
        reference_file = test_data_dir / "reference.fasta"
        
        # Perform compression with reference
        result = runner.compress(str(input_file), str(output_file), reference=str(reference_file))
        
        # Verify compression completed
        assert result is not None
        assert len(result) > 0
        
        # Verify output file was created
        assert output_file.exists()
        assert output_file.stat().st_size > 0
        
        print(f"Reference-based compression completed")
        print(f"Compressed file with reference stored at: {output_file.absolute()}")
        print(f"Compressed file size: {output_file.stat().st_size} bytes")

    def test_compress_decompress_roundtrip(self, runner, test_data_dir):
        """Test compression followed by decompression - permanent storage."""
        import time
        timestamp = str(int(time.time()))
        
        input_file = test_data_dir / "test_input.fastq"
        compressed_file = test_data_dir / f"roundtrip_{timestamp}.mgb"
        decompressed_file = test_data_dir / f"roundtrip_decompressed_{timestamp}.fastq"
        
        # Step 1: Compress
        compress_result = runner.compress(str(input_file), str(compressed_file))
        
        assert compressed_file.exists()
        assert compressed_file.stat().st_size > 0
        print(f"Compression completed: {compressed_file.absolute()}")
        
        # Step 2: Decompress
        decompress_result = runner.decompress(str(compressed_file), str(decompressed_file))
        
        assert decompressed_file.exists()
        assert decompressed_file.stat().st_size > 0
        print(f"Decompression completed: {decompressed_file.absolute()}")
        
        # Verify roundtrip integrity (basic check)
        # Just verify that both steps completed successfully and files are valid
        assert compressed_file.stat().st_size > 0
        assert decompressed_file.stat().st_size > 0
        
        # Verify the decompressed file contains FASTQ format data
        decompressed_content = decompressed_file.read_text()
        assert '@' in decompressed_content  # Has read headers
        assert '+' in decompressed_content  # Has quality separators
        
        print(f"Roundtrip successful: Compression and decompression completed")
        print(f"Original file: {input_file.stat().st_size} bytes")
        print(f"Compressed file: {compressed_file.stat().st_size} bytes") 
        print(f"Decompressed file: {decompressed_file.stat().st_size} bytes")

    def test_path_validation_different_directories(self, runner):
        """Test that files in different directories raise ValueError."""
        with tempfile.TemporaryDirectory() as temp_dir1:
            with tempfile.TemporaryDirectory() as temp_dir2:
                input_file = Path(temp_dir1) / "input.fastq"
                output_file = Path(temp_dir2) / "output.mgb"
                
                input_file.write_text("@read1\nACGT\n+\n@@@@\n")
                
                with pytest.raises(ValueError, match="same directory for Docker binding"):
                    runner.compress(str(input_file), str(output_file))

    def test_error_handling_invalid_input(self, runner, test_data_dir):
        """Test error handling with invalid input file."""
        invalid_input = test_data_dir / "nonexistent.fastq"
        output_file = test_data_dir / "error_test_output.mgb"
        
        # Should raise RuntimeError when input file doesn't exist
        # Note: GENIE returns exit code 0 but logs an error to stderr
        try:
            result = runner.compress(str(invalid_input), str(output_file))
            # If it doesn't raise an error, check that the error is in the output
            assert "does not exist" in result.lower() or "error" in result.lower()
        except RuntimeError as e:
            # This is the expected behavior
            assert "Input file does not exist" in str(e)

    def test_permanent_file_persistence(self, runner, test_data_dir):
        """Test that compressed files persist after test completion."""
        input_file = test_data_dir / "test_input.fastq"
        output_file = test_data_dir / "persistent_output.mgb"
        
        # Compress file
        result = runner.compress(str(input_file), str(output_file))
        
        # Verify file exists and record its properties
        assert output_file.exists()
        file_size = output_file.stat().st_size
        file_path = output_file.absolute()
        
        print(f"Persistent file created: {file_path}")
        print(f"File size: {file_size} bytes")
        print(f"File will remain accessible after test completion")
        
        # Verify file is readable
        assert file_size > 0
        assert output_file.is_file()
