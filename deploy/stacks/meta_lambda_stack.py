from constructs import Construct
from aws_cdk.aws_ecr_assets import Platform
from aws_cdk import (
    Stack,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_sqs as sqs,
    aws_lambda_destinations as destinations,
    aws_lambda_event_sources as event_sources,
    aws_secretsmanager as sm,
    BundlingOptions,
    DockerImage, Duration,
    aws_ec2 as ec2,
    aws_events_targets as targets,
    aws_events as events,
    aws_logs as logs,
    Size
)

class {INGEST_NAME}MetaLambda(Stack):

    def __init__(self, scope: Construct, id: str, lambda_env: dict, config: dict, **kwargs) -> None:
        super().__init__(scope, id, description=f"Lambda function to process {INGEST_NAME} Metadata", **kwargs)

        # Using values from config
        vpc_id = config.get('vpc_id')
        subnet_ids = config.get('vpc_subnet_id')
        sg_id = config.get('sg_id')

        # Retrieve VPC from config
        vpc = ec2.Vpc.from_lookup(self, "vpc", vpc_id=vpc_id, is_default=False)

        # Use the subnet from the config
        subnet_filter = ec2.SubnetFilter.by_ids(subnet_ids)
        # {INGEST_NAME} Meta Lambda Role
        {INGEST_NAME}_lambda_role = iam.Role(self, "{INGEST_NAME}LambdaRole",
                                    assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
                                    description="Role for {INGEST_NAME} Lambda",
                                    managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name(
                                        "service-role/AWSLambdaBasicExecutionRole")])

        # s3 permissions
        ingest_s3_bucket_names = config.get('ingest_s3_bucket_arn')
        cache_s3_bucket_names = config.get('cache_s3_bucket_arn')
        print(ingest_s3_bucket_names)
        print(cache_s3_bucket_names)
        # combine bucket names into one list
        bucket_names = [arn for bucket_list in [ingest_s3_bucket_names, cache_s3_bucket_names] if bucket_list is not None for arn in bucket_list]

        # s3 permissions
        {INGEST_NAME}_lambda_role.add_to_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "s3:GetObject", 
                "s3:PutObject", 
                "s3:HeadObject", 
                "s3:ListBucket",
                "s3:DeleteObject", 
                "s3:GetObjectAcl", 
                "s3:PutObjectAcl"
            ],
            resources=bucket_names
        ))

        
        # ENI permissions
        # Create, Delete and Describe NetworkInterfaces
        {INGEST_NAME}_lambda_role.add_to_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["ec2:CreateNetworkInterface", "ec2:DescribeNetworkInterfaces", "ec2:DeleteNetworkInterface","ec2:CreateTags"],
            resources=["*"]
        ))

        # secrets manager
        {INGEST_NAME}_lambda_role.add_to_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "secretsmanager:GetSecretValue",
                "secretsmanager:DescribeSecret",
                "secretsmanager:ListSecrets"
            ],
            resources=["*"]
        ))

        # Defines an AWS Lambda resource
        {INGEST_NAME}_lambda  = _lambda.DockerImageFunction(
                    scope=self,
                    id="{INGEST_NAME}MetaLambda",
                    # Function name on AWS
                    function_name="{INGEST_NAME}MetaLambda",
                    memory_size=256,
                    role={INGEST_NAME}_lambda_role,
                    architecture=_lambda.Architecture.ARM_64,
                    timeout=Duration.minutes(10),
                    environment=lambda_env,
                    tracing=_lambda.Tracing.ACTIVE,
                    # Use aws_cdk.aws_lambda.DockerImageCode.from_image_asset to build
                    # a docker image on deployment
                    code=_lambda.DockerImageCode.from_image_asset(
                        # Directory relative to where you execute cdk deploy
                        # contains a Dockerfile
                        directory="../meta-lambda/",
                        build_ssh="default",
                        platform=Platform.LINUX_ARM64,
                    ),
                    vpc=vpc,
                    vpc_subnets=ec2.SubnetSelection(subnet_filters=[subnet_filter]),
                    allow_public_subnet=True,
                    log_retention=logs.RetentionDays.ONE_MONTH
                )



        # Create a rule in CloudWatch Events
        # rule = events.Rule(
        #     self, "{INGEST_NAME}MetaLambdaRule",
        #     schedule=events.Schedule.cron(minute='*/5'),  # This will run the Lambda every 5 minutes
        # )
        rule = events.Rule(
            self, "{INGEST_NAME}MetaLambdaRule",
            schedule=events.Schedule.cron(week_day="MON",
                hour="0",
                minute="0"),  # This will run the Lambda every Monday at midnight
        )
        # Add the Lambda function as a target to the rule
        rule.add_target(targets.LambdaFunction(
            {INGEST_NAME}_lambda,
            retry_attempts=0  # This will prevent retries on timeout
        ))        
        # {INGEST_NAME}_lambda.add_event_source(event_source)
        # event_source_id = event_source.event_source_mapping_id
        # event_source_mapping_arn = event_source.event_source_mapping_arn