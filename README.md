# Ascend.io / Great Expectations docker image for Google Cloud storage

This image is a wrapper around official Ascend.io image to use Great Expectations validation tool.

## Build the docker image

The image is built with Github action located in this file :  .github/workflows/docker-build.yaml.

For now the image is pushed on docker hub at this address: fosk06/ascend-great-expectations-gcs:latest

The image is build on push on main branch and with git tag with the following form "v{X}.{Y}.{Z}"

With:

- X = Major version
- Y = Minor version
- Z = Correction version

## Use it on ascend.io platform  

This docker image is built for PySpark transforms on Ascend.io platform.
First you need a Google cloud storage bucket named for example "great_expectations_store" and a service account with the role "storage.admin" on this bucket.
Then upload this service account as a credentials on your Ascend.io instance and name it for example "great_expectations_sa".

Now you can create your PySpark transform on Ascend.io.
In the advanced settings> Runtime settings > container image URL set the correct docker hub image url : fosk06/ascend-great-expectations-gcs:latest

Then in the "Custom Spark Params" click on "require credentials" and chose you credential previously uploaded "great_expectations_sa".

## Write the PySpark transform

```python
# import the custom package
from ascend_great_expectations_gcs.validator import Validator

# lets admit we are working on a "customer" table, write the expectations in specific function
def expectations(validator):
  validator.expect_column_to_exist("customer_id")
  validator.expect_column_values_to_not_be_null("customer_id")
  validator.expect_column_to_exist('created_at')

# Ascend.io transform callback
def transform(spark_session: SparkSession, inputs: List[DataFrame], credentials=None):
    df = inputs[0]
    # instanciate the validator
    validator = Validator(
        name= NAME, # name of the validator
        gcp_project=PROJECT, # your GCP project
        bucket=BUCKET, # the name of your GCP bucket, for example "great_expectations_store"
        credentials=credentials, # credentials from the transform callback
    )
    validator.add_expectations(expectations)
    validator.run(df)
    return df
```
