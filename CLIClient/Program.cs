using AWSHelpers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CLIClient
{
    class Program
    {
        static void Main (string[] args)
        {
            // Used for testing the features of the Class Library
            AWSEC2Helper ec2Handle = new AWSEC2Helper ();

            // Reaching EC2 Spot Prices
            ec2Handle.GetSpotRequestPrices ("c4.xlarge", "us-east-b", "Linux/UNIX (Amazon VPC)", 2).ForEach (t => Console.WriteLine ("{0} - {1} - {2}", t.timestamp, t.productDescription, t.price));
            Console.ReadLine();
        }
    }
}
