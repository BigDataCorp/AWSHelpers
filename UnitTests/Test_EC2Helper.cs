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
    public class TestEC2Helper
    {
        // Attributes 

        public static RegionEndpoint regionEndPoint  = AWSGeneralHelper.GetRegionEndpoint ();
        public static string AMI_ID_hvm              = "ami-598e6032"; // VPC
        public static string AMI_ID_pv               = "ami-c7658fac"; // Classic
        public static string securityGroupId_VPC     = "sg-d998f2bc";
        public static string securityGroupId_Classic = "sg-7f2c3516";
        public static string instanceType            = "m3.large";
        public static string keyPair                 = "bigdata";
        public static string subnetId                = "subnet-45ed1132"; // for VPC use
        public AWSEC2Helper  aWSEC2Helper;

        // TODO: Put your own Keys
        private static String myAccessKey = "";
        private static String mySecretKey = "";

        /// <summary>
        /// Tests the Creation of a Classic Instance
        /// </summary>
        [Fact]
        public void CreateClassicInstanceTest ()
        {
            List<String> listIds = new List<string> ();
            aWSEC2Helper = new AWSEC2Helper (regionEndPoint, myAccessKey, mySecretKey);
            listIds = aWSEC2Helper.CreateClassicInstances (regionEndPoint, AMI_ID_pv, securityGroupId_Classic, keyPair, instanceType);

            Assert.NotNull (listIds.ElementAt (0));
        }

        /// <summary>
        /// Tests the finishing of an instances. Don't forget to put the id in the code,
        /// ( easiest way to do with Xunit)
        /// </summary>
        [Fact]
        public void FinishInstance ()
        {
            // TODO: Put below the id of the instance you want to finish 
            String ids = "";
            aWSEC2Helper = new AWSEC2Helper (regionEndPoint, myAccessKey, mySecretKey);
            Assert.True (aWSEC2Helper.TerminateInstance (ids));
        }


        /// <summary>
        /// Tests the Creation of a VPC Instance
        /// </summary>
        [Fact]
        public void CreateVPCInstanceTest ()
        {
            List<String> listIds = new List<string> ();
            aWSEC2Helper = new AWSEC2Helper (regionEndPoint, myAccessKey, mySecretKey);
            listIds = aWSEC2Helper.CreateVPCInstances (regionEndPoint, subnetId, AMI_ID_hvm, securityGroupId_VPC, keyPair, instanceType);

            Assert.NotNull (listIds.ElementAt (1));
        }
    }
}
