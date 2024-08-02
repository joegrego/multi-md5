#
# Joe Gregoria, August 2024
# Thanks to the Regents of the University of Michigan and UMGPT for the time and help.
#
import argparse
import hashlib
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

logging.basicConfig(format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


def calculate_file_checksum(file_path, read_size=8192, pass_through=None):
    """Calculate the MD5 checksum of a file."""
    hasher = hashlib.md5()
    try:
        with open(file_path, 'rb') as f:
            logger.debug(f"Processing {file_path}")
            for chunk in iter(lambda: f.read(read_size), b""):
                hasher.update(chunk)
        return file_path, hasher.hexdigest(), pass_through
    except FileNotFoundError:
        logger.warning(f"File Not Found: {file_path}")
        return file_path, None, pass_through


def read_checksum_file(file):
    """Read checksums from a file."""
    with open(file, 'r') as f:
        lines = f.readlines()

    for line in lines:
        if line.startswith("#"):
            logger.debug(f"skipping line: {line}")
            lines.remove(line)

    return [(line.split()[0], line.split()[1]) for line in lines]


def batch(iterable, n=1):
    """Yield successive n-sized chunks from iterable."""
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]


def batch_os_walk(directory, batch_size=1, skip_hidden=True):
    def is_hidden(path):
        return any(part.startswith('.') for part in path.split(os.sep))

    file_batch = []
    for root, dirs, files in os.walk(directory, followlinks=True):

        if skip_hidden:
            # Skip hidden directories
            dirs[:] = [d for d in dirs if not d.startswith('.')]

        for file in files:
            file_path = os.path.join(root, file)
            # Optionally skip hidden files
            if not skip_hidden or not is_hidden(file_path):
                file_batch.append(file_path)

                # If the batch size is reached, yield the batch and reset the list
                if len(file_batch) == batch_size:
                    yield file_batch
                    file_batch = []

    # Yield any remaining files in the last batch
    if file_batch:
        yield file_batch


def calculate_checksums_multithread(directory_path, max_workers=5, read_size=8192, outfile=sys.stdout, dotslash="", skip_hidden=True):
    """Calculate MD5 checksums for all files in the given directory."""

    # totally arbitrary; let's go with a pretty big number.
    batch_size = max_workers * 20

    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for filenames in batch_os_walk(directory_path, batch_size, skip_hidden=skip_hidden):
                futures = []
                for filename in filenames:
                    if filename.endswith(".md5") or filename.endswith(".md5sum"):
                        logger.warning(f"Skipping probable md5 file {filename}")
                    else:
                        futures.append(executor.submit(calculate_file_checksum, filename, read_size, pass_through=None))

                for future in as_completed(futures):
                    try:
                        file_path, calculated_checksum, _ = future.result()
                        common_path = os.path.commonpath([file_path, directory_path])
                        shortened_path = os.path.relpath(file_path, common_path)
                        print(f"{calculated_checksum}  {dotslash}{shortened_path}", file=outfile, flush=True)
                        logger.info(f"{calculated_checksum}  {dotslash}{shortened_path}")
                    except Exception as e:
                        raise RuntimeError(f"Error processing file: {e}")

    except KeyboardInterrupt:
        logger.warning("\nOperation cancelled by user. Exiting...")
        for future in futures:
            future.cancel()  # Cancel any pending futures
        sys.exit(1)  # Exit with error code 1


def verify_checksums_multithread(checksums, directory_path, max_workers=5, read_size=8192, keep_going=False):
    results = []

    # This batch size is totally arbitrary; it could totally be a different number.
    # I'm figuring that we want to use batches, otherwise it will submit hundreds of "executors" before processing the first few.
    # so maybe this is faster?
    batch_size = max_workers * 4

    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:

            for chunk in batch(checksums, batch_size):
                future_to_file = {executor.submit(calculate_file_checksum, os.path.abspath(os.path.join(directory_path, file)), read_size, pass_through=expected_checksum): file for
                                  expected_checksum, file in chunk}

                for future in as_completed(future_to_file):
                    file_path = future_to_file[future]
                    try:
                        file_path, calculated_checksum, expected_checksum = future.result()
                        results.append((file_path, calculated_checksum, expected_checksum))
                        if expected_checksum == calculated_checksum:
                            logger.info(f"{file_path} : OK")
                        else:
                            logger.critical(f"{file_path} : FAILED")
                            if not keep_going:
                                raise ValueError(f"Invalid checksum for {file_path}, expected={expected_checksum} calculated={calculated_checksum}")
                    except Exception as exc:
                        raise RuntimeError(f'{file_path} generated an exception: {exc}')

    except KeyboardInterrupt:
        logger.warning("\nOperation cancelled by user. Exiting...")
        for future in future_to_file:
            future.cancel()  # Cancel any pending futures
        sys.exit(1)  # Exit with error code 1

    return results


def main():
    parser = argparse.ArgumentParser(description="Calculate MD5 checksums for all files in a directory.")
    parser.add_argument("directory", type=str, help="The path to the directory.")
    # logging. I usually run with -v
    parser.add_argument("--verbose", "-v", action='store_true', help="Turn on verbose logging.")
    parser.add_argument("--debug", "-d", action='store_true', help="Turn on debug logging.")
    # you probably don't want to actually set these
    parser.add_argument("--workers", "-w", type=int, default=5, help="Number of workers to use.")
    parser.add_argument("--read-size", "-r", type=int, default=0, help="How big the file read chunks will be. Pass 0 to use os.stat to make a good guess.")
    # These only apply to "create" (that is, not-verify)
    parser.add_argument("--output-file", "-o", type=str, default=None, help="Output file name; will default to the <directory>.md5")
    parser.add_argument("--include-hidden", "-s", action='store_true', help="Include hidden files and directories; they are excluded by default")
    parser.add_argument("--nodotslash", action='store_true', help="turn off the dot-slash ('./') before each file in the checksum output")
    # these only apply to "verify" (that is, not-create)
    parser.add_argument("--verify", type=str, help="verify directory against the specified file.")
    parser.add_argument("--keepgoing", action='store_true', help="during verify, keep going even if there is a checksum failure")
    args = parser.parse_args()

    logger.setLevel(logging.WARNING)
    if args.verbose:
        logger.setLevel(logging.INFO)
    if args.debug:
        logger.setLevel(logging.DEBUG)
        logger.debug("Debug logging enabled")

    directory = os.path.abspath(os.path.normpath(args.directory))
    logger.info(f"Processing directory {directory}")

    read_size = args.read_size
    if read_size <= 0:
        try:
            read_size = os.stat(args.directory).st_blksize
        except OSError:
            read_size = 1024
            logger.debug(f"Couldn't stat the target directory, using read size of {read_size}")
    logger.debug(f"Using a read chunk size of {read_size}")

    there_was_an_error = False

    start = datetime.now().replace(microsecond=0)

    if args.verify:
        checksum_file = os.path.abspath(args.verify)
        checksums = read_checksum_file(checksum_file)
        logger.info(f"Verifying {len(checksums)} checksums from file {checksum_file} against {directory}")

        results = verify_checksums_multithread(checksums, directory, max_workers=args.workers, read_size=read_size, keep_going=args.keepgoing)

        for file_path, calculated_checksum, expected_checksum in results:
            if calculated_checksum != expected_checksum:
                logger.error(f"Failed checksum for {file_path}, expected={expected_checksum}, calculated={calculated_checksum}")
                there_was_an_error = True

        if len(checksums) != len(results):
            logger.warning(f"There are {len(checksums)} in the checksum file and we processed {len(results)} from {checksum_file}")

        num_files_in_directory = sum(1 for _, _, files in os.walk(directory, followlinks=True) for f in files)
        if num_files_in_directory != len(results):
            logger.warning(f"There are {num_files_in_directory} under {directory} file and we processed {len(results)} from {checksum_file}")

        num_lines = len(checksums)
        the_file = checksum_file

    else:
        if args.nodotslash:
            dotslash = ""
        else:
            dotslash = "./"

        skip_hidden = not args.include_hidden
        logger.debug(f"Skipping hidden files: {skip_hidden}")

        outfile_name = args.output_file
        if outfile_name is None:
            outfile_name = os.path.split(os.path.abspath(directory))[1] + ".md5"

        logger.info(f"Writing MD5 checksums to {os.path.abspath(outfile_name)}")

        with open(outfile_name, 'w') as out:
            calculate_checksums_multithread(directory, max_workers=args.workers, read_size=read_size, outfile=out, dotslash=dotslash, skip_hidden=skip_hidden)

        num_lines = sum(1 for _ in open(outfile_name))
        the_file = outfile_name

    delta = datetime.now().replace(microsecond=0) - start

    logger.info(f"Took {str(delta)}. {num_lines} files in  {os.path.abspath(the_file)}")

    if there_was_an_error:
        logger.critical("FAILURE")
        exit(1)

    logger.info("Success.")


if __name__ == '__main__':
    """
    Use a multiprocessing pool to calculate MD5 checksums for all files in the given directory.
    
    Normal usage:
    python3 multi_md5.py -v /nfs/turbo/agc-data/Delivery/10566-RS_corrected
    python3 multi_md5.py -o my.md5 .
    python3 multi_md5.py --verify my.md5 . -v
    
    the output file won't be sorted.  I suggest as a post-processing: 
     sort -k2 -o the.md5 the.md5
    """
    main()
