AWS Helpers Library
======================

This library contains helper methods of making it easier for developers to tap into the AWS SDK. Currently there's support for the following Services:

* EC2
* SQS
* S3
* CloudSearch


CLI Client
======================

This project is nothing but an `empty` Console Application that can be used as a `Client` of the AWS Helpers Library. You can play around with it and write the code there, until you are confortable
with including it into your own project.

Some Usage Examples
======================

    // Terminating EC2 Instance (or instances all at once)
    awsEC2Helper = new AWSEC2Helper (RegionEndpoint.SAEast1, myAccessKey, mySecretKey);
    awsEC2Helper.TerminateInstance (ids))
    
    // Destroying SQS Queue - Static Method
    AWSSQSHelper.DestroySQSQueue (queueName, RegionEndpoint.SAEast1, out ErrorMessage, myAccessKey, mySecretKey)

    // Batch Enqueue of SQS Messages
    var messages = List<String>();
    AWSSQSHelper queueTestAWSSQS = new AWSSQSHelper (queueName, 10, RegionEndpoint.SAEast1, myAccessKey, mySecretKey);
    queueTestAWSSQS.EnqueueMessages (messages);

App.Config
======================

You can either store your credentials on the App.Config file, using the following attributes:

    <appSettings>
     <!-- AWS Credentials -->
     <add key="AWSSecretKey"     value="YOUR KEYS HERE" />
     <add key="AWSAccessKey"     value="YOUR KEYS HERE" />
    </appSettings
  
Or you can feed each helper using the credentials yourself.

Using the App.Config is recommended because the AWS SDK will automatically reach out to this credentials, without you having to do nothing but dump them there.