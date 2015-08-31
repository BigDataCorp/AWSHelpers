using Amazon;
using AWSHelpers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace UnitTests
{
    public class Test_SQSHelper
    {
        // Attributes        

        public static String queueName = "queueTestAWSSQS";
        public static RegionEndpoint regionEndPoint = AWSGeneralHelper.GetRegionEndpoint ();
        public static String ErrorMessage = "Fail to destroy the queue";

        // // TODO: Put your own Keys
        public static String myAccessKey = "";
        public static String mySecretKey = "";

        /// <summary>
        /// Testing of a creation of a queue named queueTestAWSSQS
        /// </summary>
        [Fact]
        static void CreateSQSQueueTest ()
        {


            int AmazonSQSMaxMessageSize = 256 * 1024;
            String ErrorMessage = "Fail";
            int DelaySeconds = 1;
            int MaximumMessageSize = AmazonSQSMaxMessageSize;
            int MessageRetentionPeriod = 60 * 60 * 24 * 14; // 14 days
            int ReceiveMessageWaitTimeSeconds = 1;
            int VisibilityTimeout = 43200;
            string Policy = "";

            Assert.True (AWSSQSHelper.CreateSQSQueue (queueName, regionEndPoint, out ErrorMessage, DelaySeconds, MaximumMessageSize, MessageRetentionPeriod,
                                                        ReceiveMessageWaitTimeSeconds, VisibilityTimeout, Policy, myAccessKey, mySecretKey));
        }


        /// <summary>
        ///  Testing Deletion of the queue
        /// </summary>
        [Fact]
        static void DeleteSQSQueueTest ()
        {
            Assert.False (AWSSQSHelper.DestroySQSQueue (queueName, regionEndPoint, out ErrorMessage, myAccessKey, mySecretKey));
        }


        /// <summary>
        /// Creates a message , enqueues it , dequeues it , verify it's the good one, and then delete it
        /// The queue must be empty before testing in order for the test to be true
        /// </summary>
        [Fact]
        public void QueueMessageTest ()
        {
            String msgbody = "First test To Queue";

            // Accessing the Queue
            AWSSQSHelper queueTestAWSSQS = new AWSSQSHelper (queueName, 2, regionEndPoint, myAccessKey, mySecretKey);

            // Adding msgs
            Assert.True (queueTestAWSSQS.EnqueueMessage (msgbody));

            // Request messages
            queueTestAWSSQS.DeQueueMessages ();

            // Have i receved any ?
            Assert.True (queueTestAWSSQS.AnyMessageReceived ());

            // Asserting they are the right one
            foreach (var msg in queueTestAWSSQS.GetMessages ())
            {
                Assert.True (msg.Equals (msgbody));
                queueTestAWSSQS.DeleteMessage (msg);
            }

        }

        /// <summary>
        /// Testing the creation and the enqueuing of a batch of 10 messages
        /// </summary>
        [Fact]
        public void QueueMessageBatchTest ()
        {
            // Creating my messages list
            IList<string> messages = new List<string> ();

            for (int ind = 1; ind <= 10; ind++)
            {
                string body = "This is message number " + ind + " ! ";
                messages.Add (body);
            }

            // Accessing the queue
            AWSSQSHelper queueTestAWSSQS = new AWSSQSHelper (queueName, 10, regionEndPoint, myAccessKey, mySecretKey);

            // Adding messages
            Assert.True (queueTestAWSSQS.EnqueueMessages (messages));
        }


        /// <summary>
        /// Testing the Dequeuing of a batch of 10 messages , and deletion
        /// </summary>
        [Fact]
        public void GetQueueMessagesBatchTest ()
        {

            AWSSQSHelper queueTestAWSSQS = new AWSSQSHelper (queueName, 10, regionEndPoint, myAccessKey, mySecretKey);
            // Requesting messages
            queueTestAWSSQS.DeQueueMessages ();

            // Asserting they are the right ones
            int tamp = 1;
            foreach (var msg in queueTestAWSSQS.GetMessages ())
            {
                string msgbody = "This is message number " + tamp + " ! ";
                Assert.True (msg.Equals (msgbody));
                tamp += 1;
                //Deleting messages
                queueTestAWSSQS.DeleteMessage (msg);
            }
        }

        /// <summary>
        /// Testing the clearing of a queue
        /// </summary>

        [Fact]
        public void ClearQueueTest ()
        {
            QueueMessageBatchTest ();
            AWSSQSHelper queueTestAWSSQS = new AWSSQSHelper (queueName, 10, regionEndPoint, myAccessKey, mySecretKey);

            queueTestAWSSQS.ClearQueue ();
        }


        /// <summary>
        /// Testing the purge of a queue
        /// </summary>
        [Fact]
        public void PurgeQueueTest ()
        {
            QueueMessageBatchTest ();
            AWSSQSHelper queueTestAWSSQS = new AWSSQSHelper (queueName, 10, regionEndPoint, myAccessKey, mySecretKey);

            queueTestAWSSQS.PurgeQueue ();
        }
    }
}
