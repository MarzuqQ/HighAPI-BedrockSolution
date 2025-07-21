from aws_cdk import (
    Stack,
    CfnParameter,
    Duration,
    aws_lambda as lambda_,
    aws_lambda_event_sources as lambda_events,
    aws_sqs as sqs,
    aws_dynamodb as dynamodb,
    aws_s3 as s3,
    aws_iam as iam,
    RemovalPolicy,
    CfnOutput,
    aws_events as events,
    aws_events_targets as targets,
    aws_cloudwatch as cloudwatch,
    aws_cloudwatch_actions as cloudwatch_actions,
    aws_sns as sns,
    aws_sns_subscriptions as sns_subscriptions,
)
from constructs import Construct
from .configuration import BrRequestSmootherConfig

class BrRequestSmootherStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, config: BrRequestSmootherConfig, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Add a CDK parameter for the queue depth threshold
        queue_depth_threshold = CfnParameter(self, "QueueDepthThreshold",
            type="Number",
            default=100,
            min_value=1,
            max_value=1000,
            description="The maximum number of messages allowed in the queue before stopping processing."
        )

        # Create S3 Bucket for Bedrock request calls
        bedrock_calls_bucket = s3.Bucket(
            self, "BedrockCallsBucket",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            versioned=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
        )

        # Create DynamoDB Tables
        requests_table = dynamodb.Table(
            self, "RequestsTable",
            partition_key=dynamodb.Attribute(
                name="request_id",
                type=dynamodb.AttributeType.STRING
            ),
            stream=dynamodb.StreamViewType.NEW_AND_OLD_IMAGES,
            removal_policy=RemovalPolicy.DESTROY,
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
        )

        reporting_table = dynamodb.Table(
            self, "ReportingTable",
            partition_key=dynamodb.Attribute(
                name="report_id",
                type=dynamodb.AttributeType.STRING
            ),
            removal_policy=RemovalPolicy.DESTROY,
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
        )

        # Create SQS Queues
        input_queue = sqs.Queue(
            self, "InputQueue",
            visibility_timeout=Duration.seconds(300),
        )

        bedrock_results_queue = sqs.Queue(
            self, "BedrockResultsQueue",
            visibility_timeout=Duration.seconds(300),
        )

        # Create DLQ for hold_next_batch_queue
        hold_next_batch_dlq = sqs.Queue(
            self, "HoldNextBatchDLQ",
            visibility_timeout=Duration.seconds(300),
            # fifo=False,  # Must match the main queue
            # content_based_deduplication=True,
        )

        # Create main queue with DLQ configuration
        hold_next_batch_queue = sqs.Queue(
            self, "HoldNextBatchQueue",
            visibility_timeout=Duration.seconds(300),
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=3,  # Number of retries before message is sent to DLQ
                queue=hold_next_batch_dlq
            ),
        )

        # Add DLQ URL to outputs
        CfnOutput(
            self, "HoldNextBatchDLQUrl",
            value=hold_next_batch_dlq.queue_url,
            description="URL of the Dead Letter Queue for the FIFO batch processing queue"
        )

        # Create Lambda Functions
        request_ingestion = lambda_.Function(
            self, "RequestIngestionLambda",
            runtime=lambda_.Runtime.PYTHON_3_11,
            architecture=lambda_.Architecture.ARM_64,
            code=lambda_.Code.from_asset("lambda_/request_ingestion"),
            handler="index.handler",
            timeout=Duration.seconds(30),
            memory_size=128,
            environment={
                "REQUESTS_TABLE": requests_table.table_name,
                "S3_BUCKET": bedrock_calls_bucket.bucket_name,
            }
        )

        prioritization = lambda_.Function(
            self, "PrioritizationLambda",
            runtime=lambda_.Runtime.PYTHON_3_11,
            architecture=lambda_.Architecture.ARM_64,
            handler="index.handler",
            code=lambda_.Code.from_asset("lambda_/prioritization"),
            timeout=Duration.seconds(30),
            memory_size=128,
            environment={
                "DYNAMO_TABLE": requests_table.table_name,
                "REGION": self.region,
                "AVERAGE_REQUEST_DURATION_IN_SECONDS": str(config.average_request_duration),
                "MINUTES_TO_PROCESS": str(config.minutes_to_process),
                "DEFAULT_MESSAGE_GROUP_ID": "default",
                "HOLD_NEXT_BATCH_QUEUE_URL": hold_next_batch_queue.queue_url,
                'QUEUE_DEPTH_THRESHOLD': queue_depth_threshold.value_as_string,
            }
        )

        prepare_bedrock_call = lambda_.Function(
            self, "PrepareBedrockCallLambda",
            runtime=lambda_.Runtime.PYTHON_3_11,
            architecture=lambda_.Architecture.ARM_64,
            handler="index.handler",
            code=lambda_.Code.from_asset("lambda_/prepare_bedrock_call"),
            timeout=Duration.seconds(30),
            memory_size=128,
            reserved_concurrent_executions=config.max_concurrent_executions,
            environment={
                "DYNAMODB_TABLE_NAME": requests_table.table_name,
                "S3_BUCKET_NAME": bedrock_calls_bucket.bucket_name,
                "RESULTS_QUEUE_URL": bedrock_results_queue.queue_url,
            }
        )

        reporting = lambda_.Function(
            self, "ReportingLambda",
            runtime=lambda_.Runtime.PYTHON_3_11,
            architecture=lambda_.Architecture.ARM_64,
            handler="index.handler",
            code=lambda_.Code.from_asset("lambda_/reporting"),
            timeout=Duration.seconds(30),
            environment={
                "REPORTING_TABLE": reporting_table.table_name,
            }
        )

        # Add DynamoDB Stream trigger to reporting Lambda
        reporting.add_event_source(
            lambda_events.DynamoEventSource(
                requests_table,
                starting_position=lambda_.StartingPosition.TRIM_HORIZON,
                # batch_size=100,
                retry_attempts=3
            )
        )

        # Add Bedrock permissions to prepare_bedrock_call Lambda
        prepare_bedrock_call.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "bedrock:InvokeModel",
                    "bedrock:Converse",
                ],
                resources=["*"]  # You might want to restrict this to specific model ARNs
            )
        )

        # Add SQS trigger to request_ingestion Lambda with high concurrency
        request_ingestion.add_event_source(
            lambda_events.SqsEventSource(
                input_queue,
                batch_size=1,
                enabled=True,
   
            )
        )

        # Add SQS trigger to prepare_bedrock_call Lambda
        prepare_bedrock_call.add_event_source(
            lambda_events.SqsEventSource(
                hold_next_batch_queue,
                batch_size=1,
                enabled=True,
            )
        )


        # Grant permissions
        requests_table.grant_read_write_data(request_ingestion)
        requests_table.grant_read_write_data(prepare_bedrock_call)  # Added write permissions
        requests_table.grant_read_write_data(prioritization)
        reporting_table.grant_read_write_data(reporting)
        
        bedrock_calls_bucket.grant_read_write(request_ingestion)
        bedrock_calls_bucket.grant_read(prepare_bedrock_call)
        
        input_queue.grant_consume_messages(request_ingestion)
        bedrock_results_queue.grant_send_messages(prepare_bedrock_call)  # Grant send permissions
        bedrock_results_queue.grant_consume_messages(reporting)
        hold_next_batch_queue.grant_send_messages(prioritization)
        hold_next_batch_queue.grant_send_messages(prepare_bedrock_call)
        hold_next_batch_queue.grant_consume_messages(prepare_bedrock_call)

        # Add outputs at the end of the __init__ method
        CfnOutput(self, "InputQueueUrl",
            value=input_queue.queue_url,
            description="URL of the input SQS queue"
        )
        
        CfnOutput(self, "BedrockResultsQueueUrl",
            value=bedrock_results_queue.queue_url,
            description="URL of the Bedrock results SQS queue"
        )
        
        CfnOutput(self, "HoldNextBatchQueueUrl",
            value=hold_next_batch_queue.queue_url,
            description="URL of the FIFO queue for batch processing"
        )
        
        CfnOutput(self, "RequestsTableName",
            value=requests_table.table_name,
            description="Name of the DynamoDB requests table"
        )
        
        CfnOutput(self, "ReportingTableName",
            value=reporting_table.table_name,
            description="Name of the DynamoDB reporting table"
        )
        
        CfnOutput(self, "BedrockCallsBucketName",
            value=bedrock_calls_bucket.bucket_name,
            description="Name of the S3 bucket for Bedrock calls"
        )

        # Add Service Quotas permissions to prioritization Lambda
        prioritization.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "servicequotas:GetServiceQuota",
                    "servicequotas:ListServiceQuotas"
                ],
                resources=["*"]
            )
        )

        # Create EventBridge Rule to trigger prioritization Lambda
        events.Rule(
            self, "PrioritizationTrigger",
            schedule=events.Schedule.rate(Duration.minutes(1)),  # Changed from seconds(10) to minutes(1)
            targets=[targets.LambdaFunction(prioritization)]
        )

        # Add CloudWatch permissions to prioritization Lambda
        prioritization.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "sqs:GetQueueAttributes",
                    "cloudwatch:GetMetricData",
                    "cloudwatch:GetMetricStatistics",
                ],
                resources=["*"]
            )
        )

        # Grant permissions
        requests_table.grant_stream_read(reporting)  # Grant stream read permissions
        reporting_table.grant_write_data(reporting)  # Grant write permissions to reporting table

        # Create CloudWatch Alarm for Bedrock Invocation Errors
        bedrock_errors_alarm = cloudwatch.Alarm(
            self, "BedrockInvocationErrorsAlarm",
            metric=cloudwatch.Metric(
                namespace="AWS/Bedrock",
                metric_name="Invocation4XXErrors",
                statistic="Sum",
                period=Duration.minutes(5),
            ),
            evaluation_periods=config.alarm_evaluation_periods,
            threshold=config.alarm_threshold,
            comparison_operator=cloudwatch.ComparisonOperator.GREATER_THAN_THRESHOLD,
            alarm_description="Alert when Bedrock API invocation errors exceed threshold",
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )

        # Create SNS Topic with more detailed configuration
        alarm_topic = sns.Topic(
            self, "BedrockErrorAlarmTopic",
            display_name="Bedrock Errors Alarm",
            topic_name="bedrock-errors-alarm"  # Optional: specify a custom name
        )

        # Add email subscription to the topic
        alarm_topic.add_subscription(
            sns_subscriptions.EmailSubscription(config.notification_email)
        )

        # Add additional subscriptions if needed (SMS, Lambda, etc.)
        # alarm_topic.add_subscription(
        #     sns_subscriptions.SmsSubscription("+1234567890")
        # )

        # Connect the alarm to the SNS topic
        bedrock_errors_alarm.add_alarm_action(
            cloudwatch_actions.SnsAction(alarm_topic)
        )

        # Add CloudWatch dashboard widget (optional)
        dashboard = cloudwatch.Dashboard(
            self, "BedrockErrorsDashboard",
            dashboard_name=config.dashboard_name
        )

        dashboard.add_widgets(
            cloudwatch.GraphWidget(
                title="Bedrock API Errors",
                left=[bedrock_errors_alarm.metric],
                width=12
            )
        )

        # Output the relevant ARNs and names
        CfnOutput(
            self, "BedrockErrorsAlarmArn",
            value=bedrock_errors_alarm.alarm_arn,
            description="ARN of the Bedrock invocation errors alarm"
        )

        CfnOutput(
            self, "AlarmTopicArn",
            value=alarm_topic.topic_arn,
            description="ARN of the SNS topic for alarm notifications"
        )
