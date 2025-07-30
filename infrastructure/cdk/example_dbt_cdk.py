from aws_cdk import (
    Stack,
    aws_iam as iam,
    aws_s3 as s3,
    aws_glue as glue,
)
from constructs import Construct

class AcdcDbtStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # S3 Buckets
        clean_bucket = s3.Bucket(self, "CleanBucket", bucket_name="acdc-dbt-test-clean")
        raw_bucket = s3.Bucket(self, "RawBucket", bucket_name="acdc-dbt-test-raw")

        # Glue Databases
        db_names = [
            "acdc_dbt_test_clean",
            "acdc_dbt_test_clean_staging",
            "acdc_dbt_test_clean_intermediate",
            "acdc_dbt_test_clean_marts",
            "glue_db_acdc_dbt_test_clean",
            "glue_db_acdc_dbt_test_raw"
        ]
        glue_dbs = {
            name: glue.CfnDatabase(
                self, f"GlueDB{name.replace('_','').capitalize()}",
                catalog_id=self.account,
                database_input=glue.CfnDatabase.DatabaseInputProperty(
                    name=name
                )
            ) for name in db_names
        }

        # IAM Policy (from your YAML)
        dbt_policy = iam.Policy(self, "DbtGluePolicy",
            policy_name="dbt_glue_policy",
            statements=[
                iam.PolicyStatement(
                    sid="readAndWriteDatabases",
                    actions=[
                        "glue:SearchTables", "glue:BatchCreatePartition", "glue:CreatePartitionIndex",
                        "glue:DeleteDatabase", "glue:GetTableVersions", "glue:GetPartitions",
                        "glue:DeleteTableVersion", "glue:UpdateTable", "glue:DeleteTable",
                        "glue:DeletePartitionIndex", "glue:GetTableVersion", "glue:UpdateColumnStatisticsForTable",
                        "glue:CreatePartition", "glue:UpdateDatabase", "glue:CreateTable", "glue:GetTables",
                        "glue:GetDatabases", "glue:GetTable", "glue:GetDatabase", "glue:GetPartition",
                        "glue:UpdateColumnStatisticsForPartition", "glue:CreateDatabase", "glue:BatchDeleteTableVersion",
                        "glue:BatchDeleteTable", "glue:DeletePartition", "glue:GetUserDefinedFunctions",
                        "lakeformation:ListResources", "lakeformation:BatchGrantPermissions", "lakeformation:ListPermissions",
                        "lakeformation:GetDataAccess", "lakeformation:GrantPermissions", "lakeformation:RevokePermissions",
                        "lakeformation:BatchRevokePermissions", "lakeformation:AddLFTagsToResource",
                        "lakeformation:RemoveLFTagsFromResource", "lakeformation:GetResourceLFTags",
                        "lakeformation:ListLFTags", "lakeformation:GetLFTag"
                    ],
                    resources=[
                        "arn:aws:glue:ap-southeast-2:026090528544:catalog",
                        "arn:aws:glue:ap-southeast-2:026090528544:table/glue_db_acdc_dbt_test_clean/*",
                        "arn:aws:glue:ap-southeast-2:026090528544:database/glue_db_acdc_dbt_test_clean",
                        "arn:aws:glue:ap-southeast-2:026090528544:database/acdc_dbt_test_clean",
                        "arn:aws:glue:ap-southeast-2:026090528544:table/acdc_dbt_test_clean/*",
                        "arn:aws:glue:ap-southeast-2:026090528544:database/acdc_dbt_test_clean_staging",
                        "arn:aws:glue:ap-southeast-2:026090528544:table/acdc_dbt_test_clean_staging/*",
                        "arn:aws:glue:ap-southeast-2:026090528544:database/acdc_dbt_test_clean_intermediate",
                        "arn:aws:glue:ap-southeast-2:026090528544:table/acdc_dbt_test_clean_intermediate/*",
                        "arn:aws:glue:ap-southeast-2:026090528544:database/acdc_dbt_test_clean_marts",
                        "arn:aws:glue:ap-southeast-2:026090528544:table/acdc_dbt_test_clean_marts/*"
                    ],
                    effect=iam.Effect.ALLOW
                ),
                iam.PolicyStatement(
                    sid="readOnlyDatabases",
                    actions=[
                        "glue:SearchTables", "glue:GetTableVersions", "glue:GetPartitions",
                        "glue:GetTableVersion", "glue:GetTables", "glue:GetDatabases", "glue:GetTable",
                        "glue:GetDatabase", "glue:GetPartition", "lakeformation:ListResources", "lakeformation:ListPermissions"
                    ],
                    resources=[
                        "arn:aws:glue:ap-southeast-2:026090528544:table/glue_db_acdc_dbt_test_raw/*",
                        "arn:aws:glue:ap-southeast-2:026090528544:database/glue_db_acdc_dbt_test_raw",
                        "arn:aws:glue:ap-southeast-2:026090528544:database/default",
                        "arn:aws:glue:ap-southeast-2:026090528544:database/global_temp"
                    ],
                    effect=iam.Effect.ALLOW
                ),
                iam.PolicyStatement(
                    sid="storageAllBuckets",
                    actions=["s3:GetBucketLocation", "s3:ListBucket"],
                    resources=[
                        "arn:aws:s3:::acdc-dbt-test-clean",
                        "arn:aws:s3:::acdc-dbt-test-raw"
                    ],
                    effect=iam.Effect.ALLOW
                ),
                iam.PolicyStatement(
                    sid="readAndWriteBuckets",
                    actions=["s3:PutObject", "s3:PutObjectAcl", "s3:GetObject", "s3:DeleteObject"],
                    resources=["arn:aws:s3:::acdc-dbt-test-clean/*"],
                    effect=iam.Effect.ALLOW
                ),
                iam.PolicyStatement(
                    sid="readOnlyBuckets",
                    actions=["s3:GetObject"],
                    resources=["arn:aws:s3:::acdc-dbt-test-raw/*"],
                    effect=iam.Effect.ALLOW
                ),
            ]
        )

        # IAM Role for dbt/Glue (attach the policy)
        dbt_role = iam.Role(self, "DbtGlueRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            description="Role for dbt/Glue with custom policy"
        )
        dbt_policy.attach_to_role(dbt_role)

        # Glue Crawler for the raw bucket
        glue.CfnCrawler(
            self, "RawBucketCrawler",
            role=dbt_role.role_arn,
            database_name="glue_db_acdc_dbt_test_raw",
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[glue.CfnCrawler.S3TargetProperty(
                    path=f"s3://{raw_bucket.bucket_name}/"
                )]
            ),
            name="acdc-raw-bucket-crawler",
            description="Crawler for acdc-dbt-test-raw bucket",
            schema_change_policy=glue.CfnCrawler.SchemaChangePolicyProperty(
                update_behavior="UPDATE_IN_DATABASE",
                delete_behavior="DEPRECATE_IN_DATABASE"
            )
        )