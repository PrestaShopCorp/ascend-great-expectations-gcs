from ascend_great_expectations_gcs_test.validator import TestValidator
# from ascend_great_expectations_gcs.validator import GEValidator
from ascend_great_expectations_gcs.validator import GEValidator

with open("../.secrets/credentials.json", "r") as file:
    credentials = file.read()
    # validator = TestValidator(credentials=credentials)
    # # validator.test_run()
    # validator.test_run_checkpoint()
