K3 Dispatcher Cloud Formation
==========
Included here are a set of nested Cloud Formation templated to build your own K3 Dispatcher service, complete with one Master instance and any number of slave instances designed to run on AWS EC2. 

###Prerequisites

Ensure you have the following in place:

    - An AWS account, with proper access keys created
    - IAM Permissions to create a Cloud Formation Stack, EC2 instances, and security groups
    - An S3 bucket created to store the template files

###Creating the Stack
To create the stack:

    1. Clone this repository
    2. Upload all three YAML template files to your S3 bucket. Immediately after, copy the link to the k3-ec2-cluster.yaml file
    3. From the AWS Console, Goto: **CloudFormation** (look under Deployment & Management)
    4. Select **Create Stack**
    5. Give your Cluster a name and paste in the link to the k3-ec2-cluster.yaml file under *Specify an Amazon S3 template URL*
    6. Fill in the template parameters, optional tag, & launch the stack

We have made these templates publically availale at the following URLs:

    `https://s3-us-west-2.amazonaws.com/damsl-k3/k3-ec2-cluster.yml`
    `https://s3-us-west-2.amazonaws.com/damsl-k3/k3-ec2-master.yml`
    `https://s3-us-west-2.amazonaws.com/damsl-k3/k3-ec2-slave.yml`

###Accessing the Interface
It should take about 5-10 minutes to launch the stack. Once the instances are all running, find the Master's Public IP and browse to it on port 5000. Note that if you stop/restart the instance, the public IP will change. The CloudFormation is designed to re-establish the cluster, but you will need to find the public IP each time.
