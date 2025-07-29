import subprocess
from pathlib import Path

class DockerGenieRunner:
    def __init__(self, image="muefab/genie:latest"):
        self.image = image

    def _run_command(self, cmd):
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            # Check if there are errors in stderr even if exit code is 0
            if result.stderr and "ERROR" in result.stderr:
                print("[Genie Docker] Error:", result.stderr)
                raise RuntimeError(f"Genie Docker failed:\n{result.stderr}")
            print("[Genie Docker] Output:", result.stdout)
            return result.stdout
        except subprocess.CalledProcessError as e:
            print("[Genie Docker] Error:", e.stderr)
            raise RuntimeError(f"Genie Docker failed:\n{e.stderr}")

    def _resolve_paths(self, *paths):
        """ Resolve paths and ensure they share the same bind mount directory """
        resolved = [Path(p).resolve() for p in paths if p]
        parent_dirs = {p.parent for p in resolved}
        if len(parent_dirs) != 1:
            raise ValueError("All files must be in the same directory for Docker binding.")
        return resolved, parent_dirs.pop()

    def compress(self, input_file, output_file, reference=None, low_latency=False):
        """
        Compress file using Genie via Docker.
        Supports:
        - Global Assembly
        - Low Latency
        - Local Assembly
        - Reference-Based
        
        Args:
            input_file (str): Path to input FASTQ/SAM file
            output_file (str): Path where compressed .mgb file will be stored permanently
            reference (str, optional): Path to reference FASTA file
            low_latency (bool): Enable low-latency mode for faster compression
        
        Returns:
            str: GENIE output logs
        """
        paths, bind_dir = self._resolve_paths(input_file, output_file, reference)
        input_path, output_path = paths[0], paths[1]
        ref_path = paths[2] if reference else None

        # Ensure output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)

        cmd = [
            "docker", "run", "--rm",
            "-v", f"{bind_dir}:/work",
            self.image,
            "run",
            "-i", f"/work/{input_path.name}",
            "-o", f"/work/{output_path.name}",
            "--qv", "none",  # Skip quality value encoding to avoid quality score issues
            "-f"  # Force flag to overwrite existing files
        ]

        if low_latency:
            cmd.append("--low-latency")
        if ref_path:
            cmd.extend(["-r", f"/work/{ref_path.name}"])

        return self._run_command(cmd)

    def decompress(self, input_file, output_file, reference=None):
        """
        Decompress .mgb file using Genie via Docker.
        Automatically uses reference if needed.
        
        Args:
            input_file (str): Path to compressed .mgb file
            output_file (str): Path where decompressed FASTQ/SAM will be stored permanently
            reference (str, optional): Path to reference FASTA file (if used during compression)
        
        Returns:
            str: GENIE output logs
        """
        paths, bind_dir = self._resolve_paths(input_file, output_file, reference)
        input_path, output_path = paths[0], paths[1]
        ref_path = paths[2] if reference else None

        # Ensure output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)

        cmd = [
            "docker", "run", "--rm",
            "-v", f"{bind_dir}:/work",
            self.image,
            "run",
            "-i", f"/work/{input_path.name}",
            "-o", f"/work/{output_path.name}",
            "-f"  # Force flag to overwrite existing files
        ]

        if ref_path:
            cmd.extend(["-r", f"/work/{ref_path.name}"])

        return self._run_command(cmd)

    def version(self):
        """
        Get the version of the Genie binary inside the Docker container.
        Since there's no explicit version command, returns help output.
        """
        cmd = ["docker", "run", "--rm", self.image, "help"]
        return self._run_command(cmd).strip()
