from ascend_great_expectations_gcs_test.validator import TestValidator

with open("../.secrets/credentials.json", "r") as file:
    credentials = file.read()
    validator = TestValidator(credentials=credentials)
    validator.test_run()
    # validator.test_run_checkpoint(slack_webhook= slack_webhook)
