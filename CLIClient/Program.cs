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

            // Reaching EC2 Spot Prices for a type:c4.xlarge
            //ec2Handle.GetSpotRequestPrices ("c4.xlarge", "us-east-b", "Linux/UNIX (Amazon VPC)", 2).ForEach (t => Console.WriteLine ("{0} - {1} - {2}", t.timestamp, t.productDescription, t.price));

            List<KeyValuePair<String, String>> tags = new List<KeyValuePair<string, string>> ();
            tags.Add (new KeyValuePair<string, string> ("Nome", "Rawr"));
            tags.Add (new KeyValuePair<string, string> ("Tagging", "Taggerz"));

            ec2Handle.CreateTags (new List<String> () { "sir-02elml9r" }, tags);

            Console.ReadLine();
        }
    }
}
