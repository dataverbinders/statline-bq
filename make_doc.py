import subprocess

if __name__ == "__main__":
    subprocess.run(
        # "pwd",
        "pdoc --html --output-dir docs --template-dir docs/pdoc_templates statline_bq",
        shell=True,
    )
