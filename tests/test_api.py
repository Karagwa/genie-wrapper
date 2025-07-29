import pytest
from unittest.mock import Mock, patch
from genie_wrapper.api import compress, decompress, _get_runner
from genie_wrapper.core.native_runner import NativeGenieRunner
from genie_wrapper.core.docker_runner import DockerGenieRunner


class TestCompressFunction:
    """Test cases for the compress function."""

    @patch('genie_wrapper.api.NativeGenieRunner')
    def test_compress_native_mode_default(self, mock_native_runner):
        """Test compress function with default native mode."""
        # Arrange
        mock_runner_instance = Mock()
        mock_runner_instance.compress.return_value = "Compression successful"
        mock_native_runner.return_value = mock_runner_instance
        
        # Act
        result = compress("input.fastq", "output.mgb")
        
        # Assert
        mock_native_runner.assert_called_once()
        mock_runner_instance.compress.assert_called_once_with(
            "input.fastq", "output.mgb", reference=None, low_latency=False
        )
        assert result == "Compression successful"

    @patch('genie_wrapper.api.DockerGenieRunner')
    def test_compress_docker_mode(self, mock_docker_runner):
        """Test compress function with docker mode."""
        # Arrange
        mock_runner_instance = Mock()
        mock_runner_instance.compress.return_value = "Docker compression successful"
        mock_docker_runner.return_value = mock_runner_instance
        
        # Act
        result = compress("input.fastq", "output.mgb", mode="docker")
        
        # Assert
        mock_docker_runner.assert_called_once()
        mock_runner_instance.compress.assert_called_once_with(
            "input.fastq", "output.mgb", reference=None, low_latency=False
        )
        assert result == "Docker compression successful"

    @patch('genie_wrapper.api.NativeGenieRunner')
    def test_compress_with_reference_and_low_latency(self, mock_native_runner):
        """Test compress function with reference and low_latency parameters."""
        # Arrange
        mock_runner_instance = Mock()
        mock_runner_instance.compress.return_value = "Reference compression successful"
        mock_native_runner.return_value = mock_runner_instance
        
        # Act
        result = compress(
            "input.fastq", 
            "output.mgb", 
            reference_path="ref.fasta", 
            low_latency=True
        )
        
        # Assert
        mock_runner_instance.compress.assert_called_once_with(
            "input.fastq", "output.mgb", reference="ref.fasta", low_latency=True
        )
        assert result == "Reference compression successful"

    def test_compress_invalid_mode(self):
        """Test compress function with invalid mode raises ValueError."""
        with pytest.raises(ValueError, match="Unknown mode 'invalid'. Must be 'native' or 'docker'."):
            compress("input.fastq", "output.mgb", mode="invalid")


class TestDecompressFunction:
    """Test cases for the decompress function."""

    @patch('genie_wrapper.api.NativeGenieRunner')
    def test_decompress_native_mode_default(self, mock_native_runner):
        """Test decompress function with default native mode."""
        # Arrange
        mock_runner_instance = Mock()
        mock_runner_instance.decompress.return_value = "Decompression successful"
        mock_native_runner.return_value = mock_runner_instance
        
        # Act
        result = decompress("input.mgb", "output.fastq")
        
        # Assert
        mock_native_runner.assert_called_once()
        mock_runner_instance.decompress.assert_called_once_with(
            "input.mgb", "output.fastq", reference=None
        )
        assert result == "Decompression successful"

    @patch('genie_wrapper.api.DockerGenieRunner')
    def test_decompress_docker_mode(self, mock_docker_runner):
        """Test decompress function with docker mode."""
        # Arrange
        mock_runner_instance = Mock()
        mock_runner_instance.decompress.return_value = "Docker decompression successful"
        mock_docker_runner.return_value = mock_runner_instance
        
        # Act
        result = decompress("input.mgb", "output.fastq", mode="docker")
        
        # Assert
        mock_docker_runner.assert_called_once()
        mock_runner_instance.decompress.assert_called_once_with(
            "input.mgb", "output.fastq", reference=None
        )
        assert result == "Docker decompression successful"

    @patch('genie_wrapper.api.NativeGenieRunner')
    def test_decompress_with_reference(self, mock_native_runner):
        """Test decompress function with reference parameter."""
        # Arrange
        mock_runner_instance = Mock()
        mock_runner_instance.decompress.return_value = "Reference decompression successful"
        mock_native_runner.return_value = mock_runner_instance
        
        # Act
        result = decompress("input.mgb", "output.fastq", reference_path="ref.fasta")
        
        # Assert
        mock_runner_instance.decompress.assert_called_once_with(
            "input.mgb", "output.fastq", reference="ref.fasta"
        )
        assert result == "Reference decompression successful"

    def test_decompress_invalid_mode(self):
        """Test decompress function with invalid mode raises ValueError."""
        with pytest.raises(ValueError, match="Unknown mode 'invalid'. Must be 'native' or 'docker'."):
            decompress("input.mgb", "output.fastq", mode="invalid")


class TestGetRunnerFunction:
    """Test cases for the _get_runner helper function."""

    @patch('genie_wrapper.api.NativeGenieRunner')
    def test_get_runner_native_mode(self, mock_native_runner):
        """Test _get_runner returns NativeGenieRunner for native mode."""
        # Act
        runner = _get_runner("native")
        
        # Assert
        mock_native_runner.assert_called_once()
        assert runner == mock_native_runner.return_value

    @patch('genie_wrapper.api.DockerGenieRunner')
    def test_get_runner_docker_mode(self, mock_docker_runner):
        """Test _get_runner returns DockerGenieRunner for docker mode."""
        # Act
        runner = _get_runner("docker")
        
        # Assert
        mock_docker_runner.assert_called_once()
        assert runner == mock_docker_runner.return_value

    def test_get_runner_invalid_mode(self):
        """Test _get_runner raises ValueError for invalid mode."""
        with pytest.raises(ValueError, match="Unknown mode 'invalid'. Must be 'native' or 'docker'."):
            _get_runner("invalid")

    def test_get_runner_case_sensitive(self):
        """Test _get_runner is case sensitive."""
        with pytest.raises(ValueError, match="Unknown mode 'Native'. Must be 'native' or 'docker'."):
            _get_runner("Native")


class TestIntegration:
    """Integration-style tests that verify the full flow."""

    @patch('genie_wrapper.api.NativeGenieRunner')
    @patch('genie_wrapper.api.DockerGenieRunner')
    def test_mode_switching(self, mock_docker_runner, mock_native_runner):
        """Test that different modes use different runners."""
        # Arrange
        mock_native_instance = Mock()
        mock_docker_instance = Mock()
        mock_native_runner.return_value = mock_native_instance
        mock_docker_runner.return_value = mock_docker_instance
        
        # Act
        compress("input.fastq", "output.mgb", mode="native")
        compress("input.fastq", "output.mgb", mode="docker")
        
        # Assert
        mock_native_runner.assert_called_once()
        mock_docker_runner.assert_called_once()
        mock_native_instance.compress.assert_called_once()
        mock_docker_instance.compress.assert_called_once()

    @patch('genie_wrapper.api.NativeGenieRunner')
    def test_parameter_passing_consistency(self, mock_native_runner):
        """Test that parameters are passed consistently to both compress and decompress."""
        # Arrange
        mock_runner_instance = Mock()
        mock_native_runner.return_value = mock_runner_instance
        
        # Act
        compress("input.fastq", "output.mgb", reference_path="ref.fasta", low_latency=True)
        decompress("input.mgb", "output.fastq", reference_path="ref.fasta")
        
        # Assert
        assert mock_runner_instance.compress.call_count == 1
        assert mock_runner_instance.decompress.call_count == 1
        
        # Verify compress call
        compress_call = mock_runner_instance.compress.call_args
        assert compress_call[0] == ("input.fastq", "output.mgb")
        assert compress_call[1] == {"reference": "ref.fasta", "low_latency": True}
        
        # Verify decompress call
        decompress_call = mock_runner_instance.decompress.call_args
        assert decompress_call[0] == ("input.mgb", "output.fastq")
        assert decompress_call[1] == {"reference": "ref.fasta"}