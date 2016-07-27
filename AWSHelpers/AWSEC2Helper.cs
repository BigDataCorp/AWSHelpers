using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Amazon;
using Amazon.EC2;
using Amazon.EC2.Model;
using AWSHelpers.Models;
using System.Globalization;

namespace AWSHelpers
{
    public class AWSEC2Helper : IDisposable
    {
        private IAmazonEC2 EC2client;

        public int    ErrorCode    { get; set; }   // Last error code
        public string ErrorMessage { get; set; }   // Last error message


        /// <summary>
        /// Class constructors: default (no parameters), with region endpoint, and with endpoint + credentials
        /// </summary>
        public AWSEC2Helper ()
        {            
            Initialize (RegionEndpoint.USEast1,
                        Gadgets.LoadConfigurationSetting ("AWSAccessKey", ""),
                        Gadgets.LoadConfigurationSetting ("AWSSecretKey", ""));
        }

        public AWSEC2Helper (RegionEndpoint regionEndpoint)
        {
            Initialize (regionEndpoint,
                        Gadgets.LoadConfigurationSetting ("AWSAccessKey", ""),
                        Gadgets.LoadConfigurationSetting ("AWSSecretKey", ""));
        }

        public AWSEC2Helper (RegionEndpoint regionEndpoint, string AWSAcessKey, string AWSSecretKey)
        {
            Initialize (regionEndpoint, AWSAcessKey, AWSSecretKey);
        }

        private void Initialize (RegionEndpoint regionEndpoint, string AWSAcessKey, string AWSSecretKey)
        {
            // Set configuration info
            AmazonEC2Config config = new AmazonEC2Config ();
            config.Timeout = new TimeSpan (1, 0, 0);
            config.ReadWriteTimeout = new TimeSpan (1, 0, 0);
            config.RegionEndpoint = regionEndpoint;

            // Create EC2 client
            EC2client = AWSClientFactory.CreateAmazonEC2Client (
                            AWSAcessKey,
                            AWSSecretKey,
                            config);
        }

        /// <summary>
        /// Class disposer (to implement IDisposable)
        /// </summary>
        public void Dispose ()
        {
            try
            {
                if (EC2client != null)
                    EC2client.Dispose ();
            }
            catch
            {
            }
            EC2client = null;
        }

        /// <summary>
        /// This function associates an elastic IP to an EC2-Classic instance.
        /// </summary>
        /// <param name="InstanceId">The ID of the instance to be associated to the elastic IP</param>
        /// <param name="PublicId">The public ip ("Elastic IP" column in the AWS console)</param>
        public void AssociateElasticIpToClassicInstance (string InstanceId, string PublicId)
        {
            // Initializing request
            AssociateAddressRequest associateRequest = new AssociateAddressRequest ();
            associateRequest.InstanceId     = InstanceId;
            associateRequest.PublicIp       = PublicId;

            // Associating address & fetching response
            EC2client.AssociateAddress (associateRequest);
        }

        /// <summary>
        /// This function associates an elastic IP to an EC2-VPC instance.
        /// </summary>
        /// <param name="InstanceId">The ID of the instance to be associated to the elastic IP</param>
        /// <param name="AllocationId">The allocation id ("Allocation ID" column in the AWS console)</param>
        public void AssociateElasticIpToVpcInstance (string InstanceId, string AllocationId)
        {
            // Initializing request
            AssociateAddressRequest associateRequest = new AssociateAddressRequest ();
            associateRequest.InstanceId     = InstanceId;
            associateRequest.AllocationId   = AllocationId;

            // Associating address & fetching response
            EC2client.AssociateAddress (associateRequest);
        }

        /// <summary>
        /// This function creates a set of instances into EC2 Classic. It returns the Ids of the created instances if successful, or 
        /// sets the error code and message otherwise
        /// </summary>
        /// <param name="regionEndpoint">Region where instances should be created</param>
        /// <param name="AMI_ID">Id of the AMI that will be used as a base for the instances</param>
        /// <param name="SecurityGroupId">The name of the security group to be assigned to the instance(s)</param>
        /// <param name="KeyPairName">The name of the keypair to be assigned to the instance(s)</param>
        /// <param name="InstanceType">The type of the instance(s)</param>
        /// <param name="InstanceCount">The number of instances to be launched</param>
        /// <param name="UserData">The user-data script that will be run as the instance(s) is(are) initialized</param>
        /// <returns>The list of Instance Ids if successful</returns>
        public List<string> CreateClassicInstances (RegionEndpoint regionEndpoint, string AMI_ID, string SecurityGroupId, string KeyPairName, string InstanceType, int InstanceCount = 1, string UserData = "")
        {
            List<string> InstanceIds = new List<string> ();

            // Initialize error values
            ErrorCode    = 0;
            ErrorMessage = "";

            // Create the request object
            List<string> groups = new List<string> () { SecurityGroupId };
            var launchRequest = new RunInstancesRequest ()
            {
                ImageId          = AMI_ID,
                InstanceType     = InstanceType,
                MinCount         = InstanceCount,
                MaxCount         = InstanceCount,
                KeyName          = KeyPairName,
                SecurityGroupIds = groups,
                UserData         = Gadgets.Base64Encode (UserData)
            };

            // Launch the instances
            try
            {
                var launchResponse = EC2client.RunInstances (launchRequest);

                // Check response for errors
                if (launchResponse.HttpStatusCode != HttpStatusCode.OK)
                {
                    ErrorCode = Convert.ToInt32 (launchResponse.HttpStatusCode);
                    ErrorMessage = "Http Error [" + launchResponse.HttpStatusCode.ToString () + "]";
                }
                else
                {
                    List<Instance> createdInstances = launchResponse.Reservation.Instances;
                    foreach (Instance instance in createdInstances)
                    {
                        InstanceIds.Add (instance.InstanceId);
                    }
                }
            }
            catch (Exception ex)
            {
                ErrorCode    = -1;
                ErrorMessage = ex.Message + "::" + ex.InnerException;
            }

            return InstanceIds;
        }

        /// <summary>
        /// This function creates a set of instances into an EC2 VPC. It returns the Ids of the created instances if successful, or 
        /// sets the error code and message otherwise
        /// </summary>
        /// <param name="regionEndpoint">Region where instances should be created</param>
        /// <param name="SubnetId">Id of the VPC subnet where the instances will be launched</param>
        /// <param name="AMI_ID">Id of the AMI that will be used as a base for the instances</param>
        /// <param name="SecurityGroupId">The name of the security group to be assigned to the instance(s)</param>
        /// <param name="KeyPairName">The name of the keypair to be assigned to the instance(s)</param>
        /// <param name="InstanceType">The type of the instance(s)</param>
        /// <param name="InstanceCount">The number of instances to be launched</param>
        /// <param name="UserData">The user-data script that will be run as the instance(s) is(are) initialized</param>
        /// <returns>The list of Instance Ids if successful</returns>
        public List<string> CreateVPCInstances (RegionEndpoint regionEndpoint, string SubnetId, string AMI_ID, string SecurityGroupId, string KeyPairName, string InstanceType, int InstanceCount = 1, string UserData = "")
        {
            List<string> InstanceIds = new List<string> ();

            // Initialize error values
            ErrorCode    = 0;
            ErrorMessage = "";

            // Create the list with security groups
            List<string> SecurityGroups = new List<string> () { SecurityGroupId }; 

            // Create the network interface object (to connect with the VPC)
            var NetworkInterface = new InstanceNetworkInterfaceSpecification ()
            {
                DeviceIndex              = 0,
                SubnetId                 = SubnetId,
                Groups                   = SecurityGroups,
                AssociatePublicIpAddress = true
            };
            List<InstanceNetworkInterfaceSpecification> NetworkInterfaces = new List<InstanceNetworkInterfaceSpecification> () { NetworkInterface };

            // Create the request object            
            var launchRequest = new RunInstancesRequest ()
            {
                ImageId           = AMI_ID,
                InstanceType      = InstanceType,
                MinCount          = InstanceCount,
                MaxCount          = InstanceCount,
                KeyName           = KeyPairName,
                NetworkInterfaces = NetworkInterfaces,
                UserData          = Gadgets.Base64Encode (UserData)
            };

            // Launch the instances
            try
            {
                var launchResponse = EC2client.RunInstances (launchRequest);

                // Check response for errors
                if (launchResponse.HttpStatusCode != HttpStatusCode.OK)
                {
                    ErrorCode = Convert.ToInt32 (launchResponse.HttpStatusCode);
                    ErrorMessage = "Http Error [" + launchResponse.HttpStatusCode.ToString () + "]";
                }
                else
                {
                    List<Instance> createdInstances = launchResponse.Reservation.Instances;
                    foreach (Instance instance in createdInstances)
                    {
                        InstanceIds.Add (instance.InstanceId);
                    }
                }
            }
            catch (Exception ex)
            {
                ErrorCode    = -1;
                ErrorMessage = ex.Message + "::" + ex.InnerException;
            }

            return InstanceIds;
        }

        /// <summary>
        /// This function creates a spot instance request inside a VPC. It returns the request ID if successful, or sets the error
        /// code and message otherwise
        /// </summary>
        /// <param name="AvailabilityZone">Name of the Availability Zone where the instances will be launched</param>
        /// <param name="AMI_ID">Id of the AMI that will be used as a base for the instances</param>
        /// <param name="SecurityGroupId">The name of the security group to be assigned to the instance(s)</param>
        /// <param name="KeyPairName">The name of the keypair to be assigned to the instance(s)</param>
        /// <param name="InstanceType">The type of the instance(s)</param>
        /// <param name="InstancePrice">The max price to pay for the instance(s)</param>
        /// <param name="InstanceCount">The number of instances to be launched</param>
        /// <param name="UserData">The user-data script that will be run as the instance(s) is(are) initialized</param>
        /// <returns>The list of Request Ids if successful</returns>
        public List<string> RequestClassicSpotInstances (string AvailabilityZone, string AMI_ID, string SecurityGroupId, string KeyPairName, string InstanceType, double InstancePrice, int InstanceCount = 1, string UserData = "")
        {
            List<string> RequestIds = new List<string> ();

            // Initialize error values
            ErrorCode    = 0;
            ErrorMessage = "";

            // Create the list with security groups
            List<string> SecurityGroups = new List<string> () { SecurityGroupId };

            // Create placement object
            SpotPlacement spotPlacement = new SpotPlacement ()
            {
                AvailabilityZone = AvailabilityZone
            };

            // Create the launch specification
            LaunchSpecification launchSpecification = new LaunchSpecification ()
            {
                ImageId           = AMI_ID,
                InstanceType      = InstanceType,
                KeyName           = KeyPairName,
                SecurityGroups    = SecurityGroups,
                Placement         = spotPlacement,
                UserData          = Gadgets.Base64Encode (UserData)
            };

            // Create the request object        
            RequestSpotInstancesRequest spotRequest = new RequestSpotInstancesRequest ()
            {
                SpotPrice           = InstancePrice.ToString (),
                InstanceCount       = InstanceCount,
                LaunchSpecification = launchSpecification                
            };
            
            // Request the instances
            try
            {
                var spotResponse = EC2client.RequestSpotInstances (spotRequest);    

                // Check response for errors
                if (spotResponse.HttpStatusCode != HttpStatusCode.OK)
                {
                    ErrorCode    = Convert.ToInt32 (spotResponse.HttpStatusCode);
                    ErrorMessage = "Http Error [" + spotResponse.HttpStatusCode.ToString () + "]";
                }
                else
                {
                    foreach (SpotInstanceRequest request in spotResponse.SpotInstanceRequests)
                    {
                        RequestIds.Add (request.SpotInstanceRequestId);
                    }
                }
            }
            catch (Exception ex)
            {
                ErrorCode    = -1;
                ErrorMessage = ex.Message + "::" + ex.InnerException;
            }

            return RequestIds;
        }

        /// <summary>
        /// This function creates a spot instance request inside a VPC. It returns the request ID if successful, or sets the error
        /// code and message otherwise
        /// </summary>
        /// <param name="SubnetId">Id of the VPC subnet where the instances will be launched</param>
        /// <param name="AMI_ID">Id of the AMI that will be used as a base for the instances</param>
        /// <param name="SecurityGroupId">The name of the security group to be assigned to the instance(s)</param>
        /// <param name="KeyPairName">The name of the keypair to be assigned to the instance(s)</param>
        /// <param name="InstanceType">The type of the instance(s)</param>
        /// <param name="InstancePrice">The max price to pay for the instance(s)</param>
        /// <param name="InstanceCount">The number of instances to be launched</param>
        /// <param name="UserData">The user-data script that will be run as the instance(s) is(are) initialized</param>
        /// <returns>The list of Request Ids if successful</returns>
        public List<string> RequestVPCSpotInstances (string SubnetId, string AMI_ID, string SecurityGroupId, string KeyPairName, string InstanceType, double InstancePrice, int InstanceCount = 1, string UserData = "")
        {
            List<string> RequestIds = new List<string> ();

            // Initialize error values
            ErrorCode    = 0;
            ErrorMessage = "";

            // Create the list with security groups
            List<string> SecurityGroups = new List<string> () { SecurityGroupId };

            // Create the network interface object (to connect with the VPC)
            var NetworkInterface = new InstanceNetworkInterfaceSpecification ()
            {
                DeviceIndex              = 0,
                SubnetId                 = SubnetId,
                Groups                   = SecurityGroups,
                AssociatePublicIpAddress = true
            };
            List<InstanceNetworkInterfaceSpecification> NetworkInterfaces = new List<InstanceNetworkInterfaceSpecification> () { NetworkInterface };

            LaunchSpecification launchSpecification = new LaunchSpecification ()
            {
                ImageId           = AMI_ID,
                InstanceType      = InstanceType,
                KeyName           = KeyPairName,
                NetworkInterfaces = NetworkInterfaces,
                UserData          = Gadgets.Base64Encode (UserData)
            };

            // Create the request object        
            RequestSpotInstancesRequest spotRequest = new RequestSpotInstancesRequest ()
            {
                SpotPrice           = InstancePrice.ToString(CultureInfo.InvariantCulture),
                InstanceCount       = InstanceCount,
                LaunchSpecification = launchSpecification,
            };
            

            // Request the instances
            try
            {
                var spotResponse = EC2client.RequestSpotInstances (spotRequest);    

                // Check response for errors
                if (spotResponse.HttpStatusCode != HttpStatusCode.OK)
                {
                    ErrorCode    = Convert.ToInt32 (spotResponse.HttpStatusCode);
                    ErrorMessage = "Http Error [" + spotResponse.HttpStatusCode.ToString () + "]";
                }
                else
                {
                    foreach (SpotInstanceRequest request in spotResponse.SpotInstanceRequests)
                    {
                        RequestIds.Add (request.SpotInstanceRequestId);
                    }
                }
            }
            catch (Exception ex)
            {
                ErrorCode    = -1;
                ErrorMessage = ex.Message + "::" + ex.InnerException;
            }

            return RequestIds;
        }

        /// <summary>
        /// This function terminates an existing EC2 instance, regardless of location
        /// </summary>
        /// <param name="InstanceId">The id of the instance to be terminated</param>
        /// <returns>Success flag</returns>
        public bool TerminateInstance (string InstanceId)
        {
            bool result = false;

            // Initialize error values
            ErrorCode    = 0;
            ErrorMessage = "";

            // Create the request
            var deleteRequest = new TerminateInstancesRequest ()
            {
                InstanceIds = new List<string> { InstanceId }
            };

            try
            {
                // Run the operation
                var deleteResponse = EC2client.TerminateInstances (deleteRequest);

                // Check response for errors
                if (deleteResponse.HttpStatusCode != HttpStatusCode.OK)
                {
                    ErrorCode = Convert.ToInt32 (deleteResponse.HttpStatusCode);
                    ErrorMessage = "Http Error [" + deleteResponse.HttpStatusCode.ToString () + "]";
                }
                else
                {
                    result = true;
                }
            }
            catch (Exception ex)
            {
                ErrorCode    = -1;
                ErrorMessage = ex.Message;
            }

            return result;
        }

        /// <summary>
        /// This function returns the current state of the instance
        /// </summary>
        /// <param name="InstanceId">The id of the instance to fetch the status</param>
        /// <returns>The status of the instance</returns>
        public string GetInstanceState (string InstanceId)
        {
            string result = "";

            // Initialize error values
            ErrorCode    = 0;
            ErrorMessage = "";

            // Create request
            var instancesRequest = new DescribeInstancesRequest ();
            instancesRequest.InstanceIds = new List<string> () { InstanceId };

            try
            {
                // Run the operation
                var statusResponse = EC2client.DescribeInstances (instancesRequest);

                // Check response for errors
                if (statusResponse.HttpStatusCode != HttpStatusCode.OK)
                {
                    ErrorCode    = Convert.ToInt32 (statusResponse.HttpStatusCode);
                    ErrorMessage = "Http Error [" + statusResponse.HttpStatusCode.ToString () + "]";
                }
                else
                {
                    result = statusResponse.Reservations[0].Instances[0].State.Name;
                }
            }
            catch (Exception ex)
            {
                ErrorCode    = -1;
                ErrorMessage = ex.Message + "::" + ex.InnerException;
            }

            return result;
        }

        /// <summary>
        /// This function returns the current state of a spot request
        /// </summary>
        /// <param name="RequestId">The id of the request to fetch the status</param>
        /// <returns>The status of the spot request</returns>
        public string GetSpotRequestState (string RequestId)
        {
            string result = "";

            // Initialize error values
            ErrorCode    = 0;
            ErrorMessage = "";

            // Create request
            var instancesRequest = new DescribeSpotInstanceRequestsRequest ()
            {
                SpotInstanceRequestIds = new List<string> () { RequestId }
            };

            try
            {
                // Run the operation
                var statusResponse = EC2client.DescribeSpotInstanceRequests (instancesRequest);

                // Check response for errors
                if (statusResponse.HttpStatusCode != HttpStatusCode.OK)
                {
                    ErrorCode    = Convert.ToInt32 (statusResponse.HttpStatusCode);
                    ErrorMessage = "Http Error [" + statusResponse.HttpStatusCode.ToString () + "]";
                }
                else
                {
                    result = statusResponse.SpotInstanceRequests[0].State.Value;
                }
            }
            catch (Exception ex)
            {
                ErrorCode    = -1;
                ErrorMessage = ex.Message + "::" + ex.InnerException;
            }

            return result;        
        }

        /// <summary>
        /// This function returns the InstanceId for a given spot request
        /// </summary>
        /// <param name="RequestId">The id of the request to fetch the instance id from</param>
        /// <returns>The instance id</returns>
        public string GetInstanceIdForSpotRequest (string RequestId)
        {
            string result = "";

            // Initialize error values
            ErrorCode    = 0;
            ErrorMessage = "";

            // Create request
            var instancesRequest = new DescribeSpotInstanceRequestsRequest ()
            {
                SpotInstanceRequestIds = new List<string> () { RequestId }
            };

            try
            {
                // Run the operation
                var statusResponse = EC2client.DescribeSpotInstanceRequests (instancesRequest);

                // Check response for errors
                if (statusResponse.HttpStatusCode != HttpStatusCode.OK)
                {
                    ErrorCode    = Convert.ToInt32 (statusResponse.HttpStatusCode);
                    ErrorMessage = "Http Error [" + statusResponse.HttpStatusCode.ToString () + "]";
                }
                else
                {
                    result = statusResponse.SpotInstanceRequests[0].InstanceId;
                }
            }
            catch (Exception ex)
            {
                ErrorCode    = -1;
                ErrorMessage = ex.Message + "::" + ex.InnerException;
            }

            return result;
        }

        /// <summary>
        /// Retrieves a list of prices for the instance type received,
        /// respecting the history size parameter.
        /// </summary>
        /// <param name="instanceTypes">Type (T2.Medium, C4.XLarge...)</param>
        /// <param name="historySize">Number of prices</param>
        /// <returns>List of object containing Price, Timestamp and Description</returns>
        public List<EC2SpotPrice> GetSpotRequestPrices (String instanceType, String AZ, String productDescription = null, int historySize = 1)
        {
            // Building Requests
            var spotPriceRequest                 = new DescribeSpotPriceHistoryRequest ();
            spotPriceRequest.MaxResults          = historySize;
            spotPriceRequest.InstanceTypes       = new List<String>(){instanceType};
            spotPriceRequest.AvailabilityZone    = AZ;
            spotPriceRequest.ProductDescriptions = new List<String>(){productDescription};

            // Retrieving Spot Prices
            var response = EC2client.DescribeSpotPriceHistory (spotPriceRequest);

            //response.SpotPriceHistory.Select(t => t.ProductDescription.;
            return response.SpotPriceHistory.Select (t => new EC2SpotPrice ()
            {
                price              = float.Parse(t.Price),
                productDescription = t.ProductDescription.Value.Replace(".", ","),
                timestamp          = t.Timestamp
            }).ToList();
        }

        /// <summary>
        /// Terminates an instance based on the SpotRequest ID received
        /// </summary>
        /// <param name="spotRequestId">Id of the spot request</param>
        public void TerminateInstanceFromSpotRequest(string spotRequestId)
        {
            string instanceId = GetInstanceIdForSpotRequest (spotRequestId);
            TerminateInstance (instanceId);
        }

        /// <summary>
        /// Creates tag specified by the second parameter
        /// into the list of resources received
        /// </summary>
        /// <param name="resources">InstanceIds, SpotRequestIds...</param>
        /// <param name="tags">Tags in the form of "Key + Value"</param>
        public void CreateTags(List<String> resources, List<KeyValuePair<String,String>> tags)
        {
            var response = EC2client.CreateTags (new CreateTagsRequest ()
                {
                    Resources = resources,
                    Tags      = tags.Select(t => new Tag() { Key = t.Key, Value = t.Value}).ToList()
                });
        }

        /// <summary>
        /// Gets the instances-ids of all the EC2 instances
        /// that are 'running' based on the AMIIDs Received
        /// </summary>
        /// <param name="amiID">List of AMI-IDs used as filter for finding the instances (E.g: ami-499ad223)</param>
        /// <returns>List of the instance-ids that matched the filter and are still running</returns>
        public List<String> GetInstanceIdsForAMI (List<String> amiIDs)
        {
            // AWS SDK Request to describe instances
            DescribeInstancesRequest req = new DescribeInstancesRequest ();            
            var response                 = EC2client.DescribeInstances (req);

            List<String> instanceIdsFound = new List<String> ();

            // Iterating over instances found 
            foreach (var instanceDescription in response.Reservations)
            {
                foreach (var instance in instanceDescription.Instances)
                {
                    // Checking for filter match
                    if (amiIDs.Contains(instance.ImageId) && instance.State.Name.Value == "running")
                    {   
                        instanceIdsFound.Add (instance.InstanceId);
                    }
                }
            }

            return instanceIdsFound;
        }
    }
}
