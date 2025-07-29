from .core.native_runner import NativeGenieRunner
from .core.docker_runner import DockerGenieRunner

# MODE: Choose between "native" or "docker"
DEFAULT_MODE = "docker"  # Changed to docker for permanent file storage

def compress(input_path: str, output_path: str, reference_path: str = None,
             mode: str = DEFAULT_MODE, low_latency: bool = False):
    """
    Compress genomic data using Genie.

    Args:
        input_path (str): Path to the input FASTQ or BAM file.
        output_path (str): Path where the compressed file (.mgb) will be stored.
        reference_path (str, optional): Required for reference-based encoding.
        mode (str): Execution mode: "native" or "docker".
        low_latency (bool): Enable low-latency mode for unaligned FASTQ data.

    Returns:
        str: Output from the compression command.

    Raises:
        ValueError: If mode is invalid.
    """
    runner = _get_runner(mode)
    return runner.compress(input_path, output_path, reference=reference_path, low_latency=low_latency)


def decompress(input_path: str, output_path: str,
               reference_path: str = None, mode: str = DEFAULT_MODE):
    """
    Decompress MPEG-G files (.mgb) using Genie.

    Args:
        input_path (str): Path to the input .mgb file.
        output_path (str): Path where decompressed FASTQ or SAM will be written.
        reference_path (str, optional): Required if input was compressed with a reference.
        mode (str): Execution mode: "native" or "docker".

    Returns:
        str: Output from the decompression command.

    Raises:
        ValueError: If mode is invalid.
    """
    runner = _get_runner(mode)
    return runner.decompress(input_path, output_path, reference=reference_path)


def _get_runner(mode: str):
    """
    Return the appropriate Genie runner instance.

    Args:
        mode (str): "native" or "docker"

    Returns:
        NativeGenieRunner or DockerGenieRunner instance

    Raises:
        ValueError: If an unknown mode is provided.
    """
    if mode == "native":
        return NativeGenieRunner()
    elif mode == "docker":
        return DockerGenieRunner()
    else:
        raise ValueError(f"Unknown mode '{mode}'. Must be 'native' or 'docker'.")
