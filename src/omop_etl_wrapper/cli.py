"""Console script for omop_etl_wrapper."""
import argparse
import sys
def main():
    """Console script for omop_etl_wrapper."""
    parser = argparse.ArgumentParser()
    parser.add_argument('_', nargs='*')
    args = parser.parse_args()

    print("Arguments: " + str(args._))
    print("Replace this message by putting your code into "
          "omop_etl_wrapper.cli.main")
    return 0


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
