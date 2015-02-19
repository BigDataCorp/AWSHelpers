/*
%     *
%COPYRIGHT* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *%
%                                                                          %
% AWS Class Helpers                                                        %
%                                                                          %
% Copyright (c) 2011-2014 Big Data Corporation ©                           %
%                                                                          %
%COPYRIGHT* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *%
      *
*/
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Amazon;
using Amazon.SQS;
using Amazon.SQS.Model;

namespace AWSHelpers
{
    /// <summary>
    /// Refer to http://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/Welcome.html for the online API reference
    /// </summary>
    public class AWSSQSHelper
    {
        ///////////////////////////////////////////////////////////////////////
        //                           Fields                                  //
        ///////////////////////////////////////////////////////////////////////

        public IAmazonSQS queue                             { get; set; }   // AMAZON simple queue service reference
        public GetQueueUrlResponse queueurl                 { get; set; }   // AMAZON queue url
        public ReceiveMessageRequest rcvMessageRequest      { get; set; }   // AMAZON receive message request
        public ReceiveMessageResponse rcvMessageResponse    { get; set; }   // AMAZON receive message response
        public DeleteMessageRequest delMessageRequest       { get; set; }   // AMAZON delete message request

        public bool IsValid                                 { get; set; }   // True when the queue is OK

        public int ErrorCode                                { get; set; }   // Last error code
        public string ErrorMessage                          { get; set; }   // Last error message

        public const int e_Exception = -1;

        public const int AmazonSQSMaxMessageSize = 256 * 1024;                  // AMAZON queue max message size

        ///////////////////////////////////////////////////////////////////////
        //                    Methods & Functions                            //
        ///////////////////////////////////////////////////////////////////////

        /// <summary>
        /// This static method creates an SQS queue to be used later. For parameter definitions beyond error message, 
        /// please check the online documentation (http://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_CreateQueue.html)
        /// </summary>
        /// <param name="QueueName">Name of the queue to be created</param>
        /// <param name="RegionEndpoint">Endpoint corresponding to the AWS region where the queue should be created</param>
        /// <param name="ErrorMessage">String that will receive the error message, if an error occurs</param>
        /// <returns>Boolean indicating if the queue was created</returns>        
        public static bool CreateSQSQueue (string QueueName, RegionEndpoint RegionEndpoint, out string ErrorMessage, int DelaySeconds = 0, int MaximumMessageSize = AmazonSQSMaxMessageSize, int MessageRetentionPeriod = 345600, int ReceiveMessageWaitTimeSeconds = 0, int VisibilityTimeout = 30, string Policy = "")
        {
            bool result = false;
            ErrorMessage = "";

            // Validate and adjust input parameters
            DelaySeconds                  = Math.Min (Math.Max (DelaySeconds, 0), 900);
            MaximumMessageSize            = Math.Min (Math.Max (MaximumMessageSize, 1024), AmazonSQSMaxMessageSize);
            MessageRetentionPeriod        = Math.Min (Math.Max (MessageRetentionPeriod, 60), 1209600);
            ReceiveMessageWaitTimeSeconds = Math.Min (Math.Max (ReceiveMessageWaitTimeSeconds, 0), 20);
            VisibilityTimeout             = Math.Min (Math.Max (VisibilityTimeout, 0), 43200);

            if (!String.IsNullOrWhiteSpace (QueueName))
            {
                IAmazonSQS queueClient = AWSClientFactory.CreateAmazonSQSClient (RegionEndpoint);
                try
                {
                    // Generate the queue creation request
                    CreateQueueRequest createRequest = new CreateQueueRequest ();
                    createRequest.QueueName = QueueName;

                    // Add other creation parameters
                    createRequest.Attributes.Add ("DelaySeconds",                  DelaySeconds.ToString ());
                    createRequest.Attributes.Add ("MaximumMessageSize",            MaximumMessageSize.ToString ());
                    createRequest.Attributes.Add ("MessageRetentionPeriod",        MessageRetentionPeriod.ToString ());
                    createRequest.Attributes.Add ("ReceiveMessageWaitTimeSeconds", ReceiveMessageWaitTimeSeconds.ToString ());
                    createRequest.Attributes.Add ("VisibilityTimeout",             VisibilityTimeout.ToString ());

                    // Run the request
                    CreateQueueResponse createResponse = queueClient.CreateQueue (createRequest);

                    // Check for errros
                    if (createResponse.HttpStatusCode != System.Net.HttpStatusCode.OK)
                    {
                        ErrorMessage = "An error occurred while creating the queue. Please try again."; 
                    }

                    result = true;
                }
                catch (Exception ex)
                {
                    ErrorMessage = ex.Message;
                }
            }
            else
            {
                ErrorMessage = "Invalid Queue Name";
            }

            return result;
        }

        /// <summary>
        /// This static method deletes a SQS queue. Once deleted, the queue and any messages on it will no longer be available.
        /// </summary>
        /// <param name="QueueName">The name of the queue to be deleted</param>
        /// <param name="RegionEndpoint">Endpoint corresponding to the AWS region where the queue is located</param>
        /// <param name="ErrorMessage">String that will receive the error message, if an error occurs</param>
        /// <returns></returns>
        public static bool DestroySQSQueue (string QueueName, RegionEndpoint RegionEndpoint, out string ErrorMessage)
        {
            bool result = false;
            ErrorMessage = "";

            if (!String.IsNullOrWhiteSpace (QueueName))
            {
                IAmazonSQS queueClient = AWSClientFactory.CreateAmazonSQSClient (RegionEndpoint); 
                
                try
                {
                    // Load the queue URL
                    string url = queueClient.GetQueueUrl (QueueName).QueueUrl;

                    // Destroy the queue
                    queueClient.DeleteQueue (url);
                }
                catch (Exception ex)
                {
                    ErrorMessage = ex.Message;
                }
            }

            return result;
        }

        /// <summary>
        /// Base class constructor
        /// </summary>
        public AWSSQSHelper()
        {
        }

        /// <summary>
        /// Class constructor that initializes and opens the queue based on input parameters
        /// </summary>
        /// <param name="queueName">The name of the queue to be opened when we create the class</param>
        /// <param name="maxNumberOfMessages">The maximum number of messages that will be received upon a GET request</param>
        /// <param name="regionEndpoint">Endpoint corresponding to the AWS region where the queue we want to open resides</param>
        public AWSSQSHelper(string queueName, int maxNumberOfMessages, RegionEndpoint regionEndpoint)
        {
            OpenQueue(queueName, maxNumberOfMessages, regionEndpoint);
        }

        /// <summary>
        /// The method clears the error information associated with the queue
        /// </summary>
        private void ClearErrorInfo()
        {
            ErrorCode = 0;
            ErrorMessage = string.Empty;
        }

        /// <summary>
        /// The method opens the queue
        /// </summary>
        public bool OpenQueue(string queuename, int maxnumberofmessages, RegionEndpoint regionendpoint)
        {
            ClearErrorInfo();

            IsValid = false;

            if (!string.IsNullOrWhiteSpace(queuename))
            {
                queue = AWSClientFactory.CreateAmazonSQSClient(regionendpoint);
                try
                {
                    // Get queue url
                    GetQueueUrlRequest sqsRequest = new GetQueueUrlRequest();
                    sqsRequest.QueueName          = queuename;
                    queueurl                      = queue.GetQueueUrl(sqsRequest);

                    // Format receive messages request
                    rcvMessageRequest                     = new ReceiveMessageRequest();
                    rcvMessageRequest.QueueUrl            = queueurl.QueueUrl;
                    rcvMessageRequest.MaxNumberOfMessages = maxnumberofmessages;

                    // Format the delete messages request
                    delMessageRequest          = new DeleteMessageRequest();
                    delMessageRequest.QueueUrl = queueurl.QueueUrl;

                    IsValid = true;
                }
                catch (Exception ex)
                {
                    ErrorCode    = e_Exception;
                    ErrorMessage = ex.Message;
                }
            }

            return IsValid;
        }

        /// <summary>
        /// Returns the approximate number of queued messages
        /// </summary>
        public int ApproximateNumberOfMessages()
        {
            ClearErrorInfo();

            int result = 0;
            try
            {
                GetQueueAttributesRequest attrreq = new GetQueueAttributesRequest();
                attrreq.QueueUrl = queueurl.QueueUrl;
                attrreq.AttributeNames.Add("ApproximateNumberOfMessages");
                GetQueueAttributesResponse attrresp = queue.GetQueueAttributes(attrreq);
                if (attrresp != null)
                    result = attrresp.ApproximateNumberOfMessages;
            }
            catch (Exception ex)
            {
                ErrorCode = e_Exception;
                ErrorMessage = ex.Message;
            }

            return result;
        }

        /// <summary>
        /// The method loads a one or more messages from the queue
        /// </summary>
        public bool DeQueueMessages()
        {
            ClearErrorInfo();

            bool result = false;
            try
            {
                rcvMessageResponse = queue.ReceiveMessage(rcvMessageRequest);
                result = true;
            }
            catch (Exception ex)
            {
                ErrorCode = e_Exception;
                ErrorMessage = ex.Message;
            }
            return result;
        }

        /// <summary>
        /// The method deletes the message from the queue
        /// </summary>
        public bool DeleteMessage(Message message)
        {
            ClearErrorInfo();

            bool result = false;
            try
            {
                delMessageRequest.ReceiptHandle = message.ReceiptHandle;
                queue.DeleteMessage(delMessageRequest);
                result = true;
            }
            catch (Exception ex)
            {
                ErrorCode = e_Exception;
                ErrorMessage = ex.Message;
            }

            return result;
        }

        /// <summary>
        /// Inserts a message in the queue
        /// </summary>
        public bool EnqueueMessage(string msgbody)
        {
            ClearErrorInfo();

            bool result = false;
            try
            {
                SendMessageRequest sendMessageRequest = new SendMessageRequest();
                sendMessageRequest.QueueUrl = queueurl.QueueUrl;
                sendMessageRequest.MessageBody = msgbody;
                queue.SendMessage(sendMessageRequest);
                result = true;
            }
            catch (Exception ex)
            {
                ErrorCode = e_Exception;
                ErrorMessage = ex.Message;
            }

            return result;
        }

        /// <summary>
        /// Inserts a message in the queue and retries when an error is detected
        /// </summary>
        public bool EnqueueMessage(string msgbody, int maxretries)
        {
            // Insert domain info into queue
            bool result = false;
            int retrycount = maxretries;
            while (true)
            {
                // Try the insertion
                if (EnqueueMessage(msgbody))
                {
                    result = true;
                    break;
                }

                // Retry
                retrycount--;
                if (retrycount <= 0)
                    break;
                Thread.Sleep(Gadgets.ThreadRandomGenerator().Next(500, 2000));
            }

            // Return
            return result;
        }

        /// <summary>
        /// This method checks if any messages were received by the last call of the DeQueueMessages method
        /// </summary>
        public bool AnyMessageReceived ()
        {
            try
            {
                if (rcvMessageResponse == null)
                    return false;

                var messageResults = rcvMessageResponse.Messages;

                if (messageResults != null && messageResults.FirstOrDefault () != null)
                {
                    return true;
                }
            }
            catch
            {
                // Nothing to do here                
            }

            return false;
        }

        /// <summary>
        /// This method returns an IEnumerable (that can be iterated over) collection of messages
        /// </summary>
        public IEnumerable<Message> GetDequeuedMessages ()
        {
            return rcvMessageResponse.Messages;
        }

        /// <summary>
        /// This method repeatedly dequeues messages until there are no messages left
        /// </summary>
        public void ClearQueue ()
        {
            // TODO: We must alter the code to check how many messages are left in the queue. If there are too many messages, we should destroy the queue, wait one minute, and create it again.
            do
            {
                // Dequeueing Messages
                if (!DeQueueMessages ())
                {
                    // Checking for the need to abort (queue error)
                    if (!String.IsNullOrWhiteSpace (ErrorMessage))
                    {
                        return; // Abort
                    }

                    continue; // Continue in case de dequeue fails, to make sure no message will be kept in the queue
                }

                // Retrieving Message Results
                var resultMessages = rcvMessageResponse.Messages;

                // Checking for no message dequeued
                if (resultMessages.Count == 0)
                {
                    break; // Breaks loop
                }

                // Iterating over messages of the result to remove it
                foreach (Message message in resultMessages)
                {
                    // Deleting Message from Queue
                    DeleteMessage (message);
                }

            } while (true);
        }

        /// <summary>
        /// This method repeatedly dequeues messages from several queues until there are no messages left
        /// </summary>
        /// <param name="queueNames">The names of the queues we want to clear.</param>
        /// <param name="regionendpoint">The region endpoint for the AWS region we're using</param>
        public void ClearQueues (List<String> queueNames, RegionEndpoint regionendpoint)
        {
            // TODO: We must alter the code to check how many messages are left in the queue. If there are too many messages, we should destroy the queue, wait one minute, and create it again.

            // Iterating over queues
            foreach (string queueName in queueNames)
            {
                OpenQueue (queueName, 10, regionendpoint);

                do
                {
                    // Dequeueing Messages
                    if (!DeQueueMessages ())
                    {
                        continue; // Continue in case de dequeue fails, to make sure no message will be kept in the queue
                    }

                    // Retrieving Message Results
                    var resultMessages = rcvMessageResponse.Messages;

                    // Checking for no message dequeued
                    if (resultMessages.Count == 0)
                    {
                        break;
                    }

                    // Iterating over messages of the result to remove it
                    foreach (Message message in resultMessages)
                    {
                        // Deleting Message from Queue
                        DeleteMessage (message);
                    }

                } while (true);
            }
        }
    }
}
