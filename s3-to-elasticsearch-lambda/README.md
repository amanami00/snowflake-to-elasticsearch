# Lambda for move data from s3 to elasticsearch

* #### Set up a virtual Python environment

  When you have the local copy, you need to create a running environment. Run:

  ```bash
  # create python3.7 virtual environment
  python3.7 -m venv env
  # activate the environment
  source env/bin/activate
  ```
* #### Install dependencies
  ```bash
  # (optional) upgrade pip
  pip install --upgrade pip
  pip install -r requirements.txt -t .
  ```
  
* #### Convert into zip file
  ```bash
  zip -r lambda.zip *
  ```
