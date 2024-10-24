# Installation and Environment Configuration

## Environment Variables

The following environment variables are required to enable authentication to access the Neo4j Graph Database as well as the OPENAI API facilities.

    KGML_NEO4J_URI
    KGML_NEO4J_USER
    KGML_NEO4J_PWD

    OPENAI_API_KEY
    OPEN_AI_MODEL
    OPENAI_API_USER

NB: The OPENAI_API_USER is not required by the OPENAI API library, it is required by this moduloe in order to log and record OPENAI tolen consumption.

----

## Python Environment Configuartion

We assume that Python version 8 or superior is already installed on your system, otherwise please consult the Python installation instructions for youre p[articular operating system.

There are three steps for installing a python developmednt environment for this project:

1. You will need to clone this repository locally
2. You will need to create and activate a virtual environment
3. You will need to install the necessary project dependencies

NB: There are several tools for creating virtual environments and in the instructions that follow we use the popular **conda** tool which requires that either **anaconda** or **miniconda** first be installed on your system.

See the [conda documnentation](https://docs.conda.io/en/latest/miniconda.html) for complete installtion instructions.

Altrernatively you can use the native [venv](https://docs.python.org/3/library/venv.html) python tool without needing any addittional tools. You will need to replkace the **conda** commands below with the appropriate **venv** comands

In a Linux terminal window execute the following commands__

    git clone git@github.com:adeo/Opus-Knowledge-Graph-Python-Utils.git
    cd Opus-Knowledge-Graph-Python-Utils.git
    conda create -n myenv python==3.9
    conda activate myenv
    pip install ipython
    pip install -r requirements.txt

Your virtual environbment is now configured with the required Python libraries for this module.


-=---
