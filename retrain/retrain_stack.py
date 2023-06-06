import aws_cdk as cdk
from aws_cdk import (
    aws_iam,
    aws_s3,
    aws_ec2 as ec2,
    aws_ecs as ecs,
    aws_ecr as ecr,
    aws_batch as batch,
    aws_ecr_assets,
    aws_ssm as ssm,
    aws_lambda,
    aws_events,
    aws_events_targets as targets,
    aws_logs as logs,
    aws_cloudwatch as cloudwatch,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_codebuild as codebuild,
    aws_s3_deployment as s3deploy,
    aws_events as events,
    aws_events_targets as targets,
    Duration,
    aws_lambda_python_alpha as aws_alambda,
    Fn,
)

# WHEN RUNNING THIS STACK FOR THE FIRST TIME - MUST RUN NEPTUNE STACK FIRST AS THE BELOW ARE DEPENDENT ON NEPTUNE STACK
# import neptune security group ID from the Neptune stack output 'neptune_security_group'
neptune_stack_security_group_id = Fn.import_value("neptunesecuritygroup")
neptune_vpc_id = Fn.import_value("neptunevpcid")
neptune_private_subnet_id_1 = Fn.import_value("neptunevpcprivatesubnet1id")
neptune_private_subnet_id_2 = Fn.import_value("neptunevpcprivatesubnet2id")
neptune_cluster_writer_endpoint = Fn.import_value("neptunewriterclusterendpointname")


class RetrainStack(cdk.Stack):
    def __init__(self, scope: cdk.App, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # bucket to hold model artifacts during retraining process
        model_artifact_bucket = aws_s3.Bucket(
            self,
            "batch-job-model-artifact-bucket",
            bucket_name="model-artifacts-bucket-" + cdk.Aws.ACCOUNT_ID,
            versioned=True,
            server_access_logs_prefix="logs_",
            block_public_access=aws_s3.BlockPublicAccess.BLOCK_ALL,
            encryption=aws_s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
        )
        # bucket to hold inference artifacts during inferencing process
        inference_results_bucket = aws_s3.Bucket(
            self,
            "inference-model-results-bucket",
            bucket_name="model-inf-results-artifacts-bucket-" + cdk.Aws.ACCOUNT_ID,
            versioned=True,
            server_access_logs_prefix="logs_",
            block_public_access=aws_s3.BlockPublicAccess.BLOCK_ALL,
            encryption=aws_s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
        )
        # bucket to hold inference/retrain data for inference/retraining process
        data_bucket = aws_s3.Bucket(
            self,
            "data-bucket",
            bucket_name="model-data-bucket-" + cdk.Aws.ACCOUNT_ID,
            versioned=True,
            server_access_logs_prefix="logs_",
            block_public_access=aws_s3.BlockPublicAccess.BLOCK_ALL,
            encryption=aws_s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
        )

        # upload sample sitewise data to s3
        s3deploy.BucketDeployment(
            self,
            "data-upload-1",
            sources=[
                s3deploy.Source.asset("data"),
            ],
            destination_key_prefix = "data",
            destination_bucket=data_bucket,
        )

        # role to load data into sitewise using bulkload job api from s3
        sitewise_data_bucket_policy = aws_iam.PolicyStatement(
            actions=["s3:GetObject", "s3:GetBucketLocation", "s3:PutObject"],
            resources=[
                "arn:aws:s3:::model-data-bucket-" + cdk.Aws.ACCOUNT_ID,
                "arn:aws:s3:::model-data-bucket-" + cdk.Aws.ACCOUNT_ID + "/*",
            ],
            effect=aws_iam.Effect.ALLOW,
        )

        sitewise_s3_role = aws_iam.Role(
            self,
            "MySiteWiseS3Role",
            assumed_by=aws_iam.ServicePrincipal("iotsitewise.amazonaws.com"),
            role_name=f"sitewise-s3-role-{cdk.Aws.ACCOUNT_ID}",
        )
        sitewise_s3_role.add_to_policy(sitewise_data_bucket_policy)

        # role to allow
        aws_iam.Role(
            self,
            "neptune-read-from-s3",
            assumed_by=aws_iam.CompositePrincipal(
                aws_iam.ServicePrincipal("rds.amazonaws.com")
            ),
            role_name=f"neptune-load-from-s3-{cdk.Aws.ACCOUNT_ID}",
            managed_policies=[
                aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonS3ReadOnlyAccess"
                ),
            ],
        )

        # defined Neptune vpc for lambda querying Neptune results
        neptune_vpc = ec2.Vpc.from_vpc_attributes(self, "neptune_vpc",
            vpc_id=neptune_vpc_id,
            availability_zones=cdk.Fn.get_azs(),
            private_subnet_ids=[
                neptune_private_subnet_id_1,
                neptune_private_subnet_id_2,
            ],
        )

        # creating security group
        lambda_security_group = ec2.SecurityGroup(
            self,
            "LambdaToNeptuneSg",
            description="lambda security group",
            vpc=neptune_vpc,
        )

        # # creating IoT SiteWise endpoints for the VPC
        # neptune_vpc.add_interface_endpoint(
        #     id = "sitewise-vpc-endpoint-api",
        #     service = ec2.InterfaceVpcEndpointService("com.amazonaws.us-east-1.iotsitewise.api"),
        #     security_groups=[lambda_security_group],
        #     subnets= ec2.SubnetSelection(
        #         subnet_type = ec2.SubnetType.PRIVATE_WITH_EGRESS
        #     )
        # )

        # neptune_vpc.add_interface_endpoint(
        #     id = "sitewise-vpc-endpoint-data",
        #     service = ec2.InterfaceVpcEndpointService("com.amazonaws.us-east-1.iotsitewise.data"),
        #     security_groups=[lambda_security_group],
        #     subnets= ec2.SubnetSelection(
        #         subnet_type = ec2.SubnetType.PRIVATE_WITH_EGRESS
        #     )
        # )

        # IAM policies so multiple lambdas and batch role can access prod data processed bucket and Athena
        # bucket is encrypted in another account so need kms access
        kms_statement = aws_iam.PolicyStatement(actions=["kms:decrypt"], resources=["*"])

        # add permissions for IotSitewise
        sitewise_statement = aws_iam.PolicyStatement(
            actions=["iotsitewise:BatchGetAssetPropertyValueHistory"], resources=["*"]
        )

        # allowing function to invoke inference lambda
        invoke_inference_lambda_statement = aws_iam.PolicyStatement(
            actions=["lambda:InvokeFunction"],
            resources=[
                "arn:aws:lambda:"
                + cdk.Aws.REGION
                + ":"
                + cdk.Aws.ACCOUNT_ID
                + ":function:*-inference-lambda"
            ],
        )

        codebuild_statement = aws_iam.PolicyStatement(
            actions=["codebuild:StartBuild", "codebuild:BatchGetBuilds"], resources=["*"]
        )

        vpc_statement = aws_iam.PolicyStatement(
            effect=aws_iam.Effect.ALLOW,
            actions=[
                "ec2:CreateNetworkInterface",
                "ec2:DescribeNetworkInterfaces",
                "ec2:DeleteNetworkInterface",
                "ec2:AssignPrivateIpAddresses",
                "ec2:UnassignPrivateIpAddresses",
            ],
            resources=["*"],
        )

        # helper lambdas.

        # init lambda to prep data and invoke inference lambda
        init_lambda = aws_alambda.PythonFunction(
            self,
            "init-function",
            function_name="init-function",
            entry="./lambdas",
            runtime=aws_lambda.Runtime.PYTHON_3_9,
            index="init_lambda.py",
            handler="handler",
            timeout=cdk.Duration.minutes(15),
            environment={
                "bucket": inference_results_bucket.bucket_name,
                "data_bucket": data_bucket.bucket_name,
            },
            reserved_concurrent_executions=30,
        )

        codebuild_lambda = aws_alambda.PythonFunction(
            self,
            "codebuild-function",
            function_name="codebuild-function",
            entry="./lambdas",
            runtime=aws_lambda.Runtime.PYTHON_3_9,
            index="codebuild_lambda.py",
            handler="handler",
            timeout=cdk.Duration.minutes(15),
            reserved_concurrent_executions=30,
        )

        # helper lambda to get site ids and rtus
        site_id_and_rtu_lambda = aws_alambda.PythonFunction(
            self,
            "site-id-and-rtu-function",
            function_name="site-id-and-rtu-function",
            entry="./lambdas",
            runtime=aws_lambda.Runtime.PYTHON_3_9,
            index="site_id_and_rtu_lambda.py",
            handler="handler",
            timeout=cdk.Duration.minutes(15),
            vpc=neptune_vpc,
            security_groups=[lambda_security_group],
            environment={
                "bucket": inference_results_bucket.bucket_name,
                "data_bucket": data_bucket.bucket_name,
                "neptune_cluster_writer_endpoint": neptune_cluster_writer_endpoint,
            },
            memory_size=1024,
            reserved_concurrent_executions=30,
        )

        # orchestrating lambda to return site ids and start site_id_and_rtu_lambda
        site_id_lambda = aws_alambda.PythonFunction(
            self,
            "site-id-function",
            function_name="site-id-function",
            entry="./lambdas",
            runtime=aws_lambda.Runtime.PYTHON_3_9,
            index="site_id_lambda.py",
            handler="handler",
            timeout=cdk.Duration.minutes(5),
            vpc=neptune_vpc,
            security_groups=[lambda_security_group],
            environment={
                "bucket": inference_results_bucket.bucket_name,
                "neptune_cluster_writer_endpoint": neptune_cluster_writer_endpoint,
                "site_id_and_rtu_lambda": site_id_and_rtu_lambda.function_arn,
            },
            reserved_concurrent_executions=30,
        )
        site_id_lambda.node.add_dependency(site_id_and_rtu_lambda)

        # lookup neptune security group by the security group id
        neptune_security_group = ec2.SecurityGroup.from_security_group_id(
            self,
            "this_stack_neptune_security_group_id",
            neptune_stack_security_group_id,
        )

        # adding ingress rule to neptune sg
        neptune_security_group.add_ingress_rule(
            peer=lambda_security_group, connection=ec2.Port.tcp(8182)
        )

        # adding ingress rule to lambda sg
        lambda_security_group.add_ingress_rule(
            peer=lambda_security_group, connection=ec2.Port.all_tcp()
        )

        data_bucket.grant_read_write(site_id_lambda)
        data_bucket.grant_read_write(site_id_and_rtu_lambda)
        data_bucket.grant_read_write(init_lambda)

        site_id_and_rtu_lambda.role.add_to_policy(sitewise_statement)

        site_id_lambda.role.add_to_policy(kms_statement)
        site_id_and_rtu_lambda.role.add_to_policy(kms_statement)
        init_lambda.role.add_to_policy(kms_statement)

        # init_lambda.role.add_managed_policy(vpc_statement)
        site_id_lambda.role.add_managed_policy(vpc_statement)
        site_id_and_rtu_lambda.role.add_managed_policy(vpc_statement)

        init_lambda.role.add_to_policy(invoke_inference_lambda_statement)

        codebuild_lambda.role.add_to_policy(codebuild_statement)

        # deploy retrain EC2 in public subnet so it has internet access and to save cost on NAT Gateway. The subnet sg allows no inbound traffic for security.
        retrain_subnet_configuration = ec2.SubnetConfiguration(
            name="retrain_subnet", subnet_type=ec2.SubnetType.PUBLIC, cidr_mask=26, map_public_ip_on_launch=False,
        )

        retrain_vpc = ec2.Vpc(
            self,
            "retrain-batch-job-vpc",
            vpc_name="retrain-batch-job-vpc",
            cidr="10.0.0.0/25",
            nat_gateways=0,
            subnet_configuration=[retrain_subnet_configuration],
        )

        retrain_sg = ec2.SecurityGroup(
            self,
            "retrain-batch-job-security-group",
            vpc=retrain_vpc,
            security_group_name="retrain-batch-job-security-group",
        )

        # batch job for retraining:  retrain container image assets, iam role, launch template, AMI, compute environment, job definition and job queue.
        retrain_image_asset = aws_ecr_assets.DockerImageAsset(
            self, "retrain-image-asset", directory="retrain_image_asset"
        )

        batch_instance_role = aws_iam.Role(
            self,
            "retrain-batch-job-instance-role",
            assumed_by=aws_iam.CompositePrincipal(
                aws_iam.ServicePrincipal("ec2.amazonaws.com"),
                aws_iam.ServicePrincipal("ecs.amazonaws.com"),
                aws_iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            ),
            managed_policies=[
                aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AmazonEC2ContainerServiceforEC2Role"
                )
            ],
        )

        model_artifact_bucket.grant_read_write(batch_instance_role)

        batch_instance_role.add_to_policy(kms_statement)

        # letting the batch instance read from the data bucket
        data_bucket.grant_read(batch_instance_role)

        batch_instance_profile = aws_iam.CfnInstanceProfile(
            self,
            "retrain-batch-job-instance-profile",
            roles=[batch_instance_role.role_name],
            instance_profile_name="retrain-batch-job-instance-profile",
        )

        launch_template_data_property = (
            ec2.CfnLaunchTemplate.LaunchTemplateDataProperty(
                block_device_mappings=[
                    ec2.CfnLaunchTemplate.BlockDeviceMappingProperty(
                        device_name="/dev/xvda",
                        ebs=ec2.CfnLaunchTemplate.EbsProperty(
                            delete_on_termination=True,
                            encrypted=True,
                            volume_size=80,
                            volume_type="gp2",
                        ),
                    )
                ]
            )
        )

        lt = ec2.CfnLaunchTemplate(
            self,
            "retrain-compute-lt",
            launch_template_data=launch_template_data_property,
        )

        ecs_optimized_gpu_amznlx2_image_id = ssm.StringParameter.value_for_string_parameter(
            self,
            "/aws/service/ecs/optimized-ami/amazon-linux-2/gpu/recommended/image_id",
        )

        compute_environment = batch.CfnComputeEnvironment(
            self,
            "retrain-batch-compute-environment",
            type="MANAGED",
            compute_resources=batch.CfnComputeEnvironment.ComputeResourcesProperty(
                subnets=retrain_vpc.select_subnets(
                    subnet_type=ec2.SubnetType.PUBLIC
                ).subnet_ids,
                minv_cpus=0,
                desiredv_cpus=16,
                maxv_cpus=64,
                instance_role=batch_instance_profile.attr_arn,
                security_group_ids=[retrain_sg.security_group_id],
                type="EC2",
                instance_types=["p3", "g3", "g4dn"],
                image_id=ecs_optimized_gpu_amznlx2_image_id,
                launch_template=batch.CfnComputeEnvironment.LaunchTemplateSpecificationProperty(
                    launch_template_id=lt.ref
                ),
            ),
        )

        job_queue = batch.CfnJobQueue(
            self,
            "retrain-job-queue",
            job_queue_name="retrain-job-queue",
            priority=1000,
            compute_environment_order=[
                batch.CfnJobQueue.ComputeEnvironmentOrderProperty(
                    compute_environment=compute_environment.attr_compute_environment_arn,
                    order=1,
                )
            ],
        )

        batch_job_definition = batch.CfnJobDefinition(
            self,
            "retrain-job-definition",
            job_definition_name="retrain-job-definition",
            type="container",
            container_properties=batch.CfnJobDefinition.ContainerPropertiesProperty(
                image=retrain_image_asset.image_uri,
                resource_requirements=[
                    batch.CfnJobDefinition.ResourceRequirementProperty(
                        type="GPU", value="1"
                    ),
                    batch.CfnJobDefinition.ResourceRequirementProperty(
                        type="VCPU", value="4"
                    ),
                    batch.CfnJobDefinition.ResourceRequirementProperty(
                        type="MEMORY", value="30720"
                    ),
                ],
            ),
        )

        inference_lambda_execution_role = aws_iam.Role(
            self,
            "inference-lambda-execution-role",
            assumed_by=aws_iam.CompositePrincipal(
                aws_iam.ServicePrincipal("lambda.amazonaws.com")
            ),
            managed_policies=[
                aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                ),
                aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonTimestreamFullAccess"
                ),
            ],
        )

        inference_results_bucket.grant_read_write(inference_lambda_execution_role)

        # codebuild step to compile inference image and push to ECR
        codebuild_artifacts_bucket = aws_s3.Bucket(
            self,
            "codebuild-bucket",
            bucket_name="retrain-codebuild-artifacts-bucket-" + cdk.Aws.ACCOUNT_ID,
            versioned=True,
            server_access_logs_prefix="logs_",
            block_public_access=aws_s3.BlockPublicAccess.BLOCK_ALL,
            encryption=aws_s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
        )

        inference_image_codebuild_bucket_deployment = s3deploy.BucketDeployment(
            self,
            "inference-image-populate-codebuild-artifacts-bucket",
            sources=[
                s3deploy.Source.asset("inference_lambda"),
                s3deploy.Source.asset("inference_image_codebuild"),
            ],
            destination_bucket=codebuild_artifacts_bucket,
            destination_key_prefix="source-inference-image-build",
        )

        inference_lambda_update_codebuild_bucket_deployment = s3deploy.BucketDeployment(
            self,
            "inference-lambda-update-populate-codebuild-artifacts-bucket",
            sources=[
                s3deploy.Source.asset("create_or_update_inference_lambda_codebuild")
            ],
            destination_bucket=codebuild_artifacts_bucket,
            destination_key_prefix="source-inference-lambda-update",
        )

        inference_image_codebuild_s3_source = codebuild.Source.s3(
            bucket=codebuild_artifacts_bucket, path="source-inference-image-build/"
        )

        inference_lambda_update_codebuild_s3_source = codebuild.Source.s3(
            bucket=codebuild_artifacts_bucket, path="source-inference-lambda-update/"
        )

        retrained_inference_ecr = ecr.Repository(
            self, "retrained-inference-ecr", repository_name="retrained-inference-ecr"
        )

        inference_image_build_project = codebuild.Project(
            self,
            "inference-image-build-project",
            project_name="inference-image-build-project",
            source=inference_image_codebuild_s3_source,
            environment=codebuild.BuildEnvironment(privileged=True),
            environment_variables={
                "AWS_ACCOUNT_ID": codebuild.BuildEnvironmentVariable(
                    value=cdk.Aws.ACCOUNT_ID
                ),
                "IMAGE_REPO_NAME": codebuild.BuildEnvironmentVariable(
                    value=retrained_inference_ecr.repository_name
                ),
                "MODEL_ARTIFACTS_BUCKET": codebuild.BuildEnvironmentVariable(
                    value=model_artifact_bucket.bucket_name
                ),
            },
        )

        model_artifact_bucket.grant_read(inference_image_build_project.role)
        codebuild_artifacts_bucket.grant_read(inference_image_build_project.role)
        retrained_inference_ecr.grant_pull_push(inference_image_build_project.role)

        # codebuild step to create/update inference lambda using ECR image
        create_or_update_inference_lambda_project = codebuild.Project(
            self,
            "create-or-update-inference-lambda-project",
            project_name="create-or-update-inference-lambda-project",
            source=inference_lambda_update_codebuild_s3_source,
            environment_variables={
                "AWS_ACCOUNT_ID": codebuild.BuildEnvironmentVariable(
                    value=cdk.Aws.ACCOUNT_ID
                ),
                "IMAGE_REPO_NAME": codebuild.BuildEnvironmentVariable(
                    value=retrained_inference_ecr.repository_name
                ),
                "INFERENCE_IMAGE_URI": codebuild.BuildEnvironmentVariable(
                    value=cdk.Aws.ACCOUNT_ID
                    + ".dkr.ecr."
                    + cdk.Aws.REGION
                    + ".amazonaws.com/"
                    + retrained_inference_ecr.repository_name
                ),
                "INFERENCE_LAMBDA_EXECUTION_ROLE_ARN": codebuild.BuildEnvironmentVariable(
                    value=inference_lambda_execution_role.role_arn
                ),
                "INFERENCE_RESULTS_ARTIFACTS_BUCKET": codebuild.BuildEnvironmentVariable(
                    value=inference_results_bucket.bucket_name
                ),
            },
        )
        create_or_update_inference_lambda_project.role.attach_inline_policy(
            aws_iam.Policy(
                self,
                "inference-lambda-update-policy",
                statements=[
                    aws_iam.PolicyStatement(
                        actions=[
                            "lambda:GetFunction",
                            "lambda:CreateFunction",
                            "lambda:UpdateFunctionCode",
                        ],
                        resources=[f"arn:aws:lambda:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:function:*"],
                    ),
                    aws_iam.PolicyStatement(
                        actions=["iam:GetRole", "iam:PassRole"],
                        resources=[inference_lambda_execution_role.role_arn],
                    ),
                ],
            )
        )
        create_or_update_inference_lambda_project.role.attach_inline_policy(
            aws_iam.Policy(
                self,
                "inference-lambda-update-ecr-access-policy",
                statements=[
                    aws_iam.PolicyStatement(
                        actions=["ecr:BatchGetImage", "ecr:SetRepositoryPolicy", "ecr:GetRepositoryPolicy", "ecr:PutImage"],
                        resources=[retrained_inference_ecr.repository_arn],
                    ),
                    aws_iam.PolicyStatement(
                        actions=["ecr:GetAuthorizationToken"], resources=["*"]
                    ),
                ],
            )
        )
        # allowing the create or update inference lambda project to read the codebuild artifacts bucket
        codebuild_artifacts_bucket.grant_read(
            create_or_update_inference_lambda_project.role
        )

        # step function to tie batch retrain task and codebuild image build task together. change out to use resultspath
        site_id_task = tasks.LambdaInvoke(
            self, "Get Site IDs for model retraining", lambda_function=site_id_lambda
        )

        site_id_and_rtu_task = tasks.LambdaInvoke(
            self,
            "Get Site ID and RTU data and save to S3",
            lambda_function=site_id_and_rtu_lambda,
        )

        model_map = sfn.Map(
            self,
            "Create model per site",
            items_path="$.Payload.data",
            output_path=sfn.JsonPath.DISCARD,
        )

        retrain_batch_task = tasks.BatchSubmitJob(
            self,
            "Submit Retrain Batch Job",
            job_definition_arn=batch_job_definition.ref,
            job_name="retrain-batch-job",
            job_queue_arn=job_queue.attr_job_queue_arn,
            container_overrides=tasks.BatchContainerOverrides(
                environment={
                    "site_id": sfn.JsonPath.string_at("$.Payload.site_id"),
                    "event_id": sfn.JsonPath.string_at("$.Payload.event_id"),
                    "pipeline_type": sfn.JsonPath.string_at("$.Payload.pipeline_type"),
                    "data_bucket": data_bucket.bucket_name,
                    "model_artifact_bucket": model_artifact_bucket.bucket_name,
                }
            ),
            result_path=sfn.JsonPath.string_at("$.result"),
        )

        model_map.iterator(site_id_and_rtu_task)

        inference_image_update_task = tasks.LambdaInvoke(
            self,
            "inference-image-update",
            lambda_function=codebuild_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "project_name": inference_image_build_project.project_name,
                    "site_id": sfn.JsonPath.string_at("$.Payload.site_id"),
                }
            ),
            result_path=sfn.JsonPath.string_at("$.result"),
        )

        create_or_update_inference_lambda_task = tasks.LambdaInvoke(
            self,
            "create-or-update-inference-lambda",
            lambda_function=codebuild_lambda,
            payload=sfn.TaskInput.from_object(
                {
                    "project_name": create_or_update_inference_lambda_project.project_name,
                    "site_id": sfn.JsonPath.string_at("$.Payload.site_id"),
                }
            ),
            result_path=sfn.JsonPath.string_at("$.result"),
        )

        # defining the sequence of events inside map state
        site_id_and_rtu_task.next(retrain_batch_task)
        retrain_batch_task.next(inference_image_update_task)
        inference_image_update_task.next(create_or_update_inference_lambda_task)
        retrain_definition = site_id_task.next(model_map)

        retrain_sfn = sfn.StateMachine(
            self,
            "retrain_sfn",
            definition=retrain_definition,
            state_machine_name="retrain-pipeline",
        )

        # Inference Pipeline

        site_id_job = tasks.LambdaInvoke(
            self, "Site ID", lambda_function=site_id_lambda
        )

        site_id_and_rtu_job = tasks.LambdaInvoke(
            self, "Site ID and RTU Job", lambda_function=site_id_and_rtu_lambda
        )

        rtu_map = sfn.Map(
            self,
            "Site ID to RTU Map State",
            items_path="$.Payload.data",
            output_path=sfn.JsonPath.DISCARD,
        )

        rtu_map.iterator(site_id_and_rtu_job)

        init_job = tasks.LambdaInvoke(self, "Init Job", lambda_function=init_lambda)
        init_job_value_error = sfn.Pass(self, "Init Job Caught Value Error")
        init_job_failed = sfn.Pass(self, "Init Job Caught Unknown Exception")
        init_job.add_catch(init_job_value_error, errors=["ValueError"])
        init_job.add_catch(init_job_failed)
        init_job_succeeded = sfn.Pass(self, "Init Job Succeeded")
        init_job.next(init_job_succeeded)

        site_id_and_rtu_job.next(init_job)

        infer_definition = site_id_job.next(rtu_map)

        infer_sfn = sfn.StateMachine(
            self,
            "infer_sfn",
            definition=infer_definition,
            state_machine_name="inference-pipeline",
        )

        init_value_error_metric_filter = logs.CfnMetricFilter(
            self,
            "InitValueErrorMetricFilter",
            filter_pattern="ValueError",
            log_group_name=init_lambda.log_group.log_group_name,
            metric_transformations=[
                logs.CfnMetricFilter.MetricTransformationProperty(
                    metric_name="InitLambdaValueErrorCount",
                    metric_namespace="EoMlPipelineValueError",
                    metric_value="1",
                )
            ],
        )

        init_error_widget = cloudwatch.GraphWidget(
            title="Init Value Error Count",
            width=16,
            height=9,
            left=[
                cloudwatch.Metric(
                    namespace="EoMlPipelineValueError",
                    metric_name="InitLambdaValueErrorCount",
                    statistic="SampleCount",
                )
            ],
        )

        eo_ml_pipeline_value_error_dashboard = cloudwatch.Dashboard(
            self,
            "EoMlPipelineValueErrorDashboard",
            dashboard_name="EoMlPipelineValueErrorDashboard",
            widgets=[[init_error_widget]],
        )

        # Cloudwatch events to kick off retraining pipeline every month
        retrain_rule = events.Rule(
            self,
            "retrain-rule",
            schedule=events.Schedule.rate(Duration.days(30)),
            targets=[targets.SfnStateMachine(retrain_sfn)],
        )

        # Cloudwatch event to kick off inference pipeline every hour
        infer_rule = events.Rule(
            self,
            "infer-rule",
            schedule=events.Schedule.rate(Duration.hours(1)),
            targets=[targets.SfnStateMachine(infer_sfn)],
        )
