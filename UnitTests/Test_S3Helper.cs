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
    public class TestS3Helper
    {
        // Attributes 
        // TODO: Put your own Keys
        private static String myAccessKey = "";
        private static String mySecretKey = "";

        public static RegionEndpoint regionEndPoint = AWSGeneralHelper.GetRegionEndpoint ();

        // // TODO: Put the Bucket name you want to test
        public static String bucketname = "";

        /// <summary>
        /// Tests the saving of a String in the S3
        /// </summary>
        [Fact]
        public void SaveStringTest ()
        {
            AWSS3Helper awss3Helper = new AWSS3Helper (regionEndPoint, myAccessKey, mySecretKey);
            string datavalue = "First Test for S3";
            string dataname  = "First Try";

            Assert.True (awss3Helper.SaveString (bucketname, dataname, datavalue));

        }

        /// <summary>
        /// Test the saving of a file in the S3
        /// </summary>
        [Fact]
        public void SaveFileTest ()
        {
            AWSS3Helper awss3Helper = new AWSS3Helper (regionEndPoint, myAccessKey, mySecretKey);
            // TODO: Put the Path of the file
            string datapath = @"";
            string dataname = "Excel Test File";

            Assert.True (awss3Helper.SaveFile (bucketname, dataname, datapath));

        }

        /// <summary>
        /// Tests the loading of a String from the S3
        /// </summary>
        [Fact]
        public void LoadStringTest ()
        {
            AWSS3Helper awss3Helper = new AWSS3Helper (regionEndPoint, myAccessKey, mySecretKey);
            string dataname = "First Try";
            string datavalue = "";
            Assert.True (awss3Helper.LoadString (bucketname, dataname, out datavalue));
        }

        /// <summary>
        /// Tests the dowloading of a file from the S3
        /// </summary>
        [Fact]
        public void DownloadFileTest ()
        {
            AWSS3Helper awss3Helper = new AWSS3Helper (regionEndPoint, myAccessKey, mySecretKey);
            // TODO: Put the Path of the file
            string dataname = "Excel Test File";
            string filepath = @"";
            Assert.True (awss3Helper.FileDownload (bucketname, dataname, filepath));
        }

        /// <summary>
        /// Tests the deletion of data
        /// </summary>
        [Fact]
        public void DeleteDataTest ()
        {
            AWSS3Helper awss3Helper = new AWSS3Helper (regionEndPoint, myAccessKey, mySecretKey);
            string dataname = "Excel Test File"; ;
            Assert.True (awss3Helper.DeleteDataItem (bucketname, dataname));
        }

        /// <summary>
        /// Tests the existence of an item in the bucket
        /// </summary>
        [Fact]
        public void DataItemExistTest ()
        {
            AWSS3Helper awss3Helper = new AWSS3Helper (regionEndPoint, myAccessKey, mySecretKey);
            string dataname = "Excel Test File";
            Assert.False (awss3Helper.DataItemExists (bucketname, dataname));
            dataname = "First Try";
            Assert.True (awss3Helper.DataItemExists (bucketname, dataname));
        }

        /// <summary>
        /// Test the return of a list with all the items from the bucket
        /// There are two definitions : you can either define the Callback properly 
        /// Or define it anonymously
        /// </summary>
        [Fact]
        public void DataListItemTest ()
        {
            AWSS3Helper awss3Helper = new AWSS3Helper (regionEndPoint, myAccessKey, mySecretKey);
            string datanameExcel = "Excel Test File";
            string datanameString = "First Try";
            List<string> dataitems = new List<string> ();

            //awss3Helper.ListDataItems (bucketname, dataitems, NotificationCallback);

            awss3Helper.ListDataItems (bucketname, dataitems, delegate (int count, out bool shouldAbort) { shouldAbort = false; });

            Assert.True (dataitems.ElementAt (0).Equals (datanameExcel));
            Assert.True (dataitems.ElementAt (1).Equals (datanameString));
        }

        int total = 0;

        /// <summary>
        /// Proper definition of the Callback used before
        /// </summary>
        /// <param name="count"></param>
        /// <param name="shouldAbort"></param>
        public void NotificationCallback (int count, out bool shouldAbort)
        {
            total += count;
            shouldAbort = false;
        }
    }
}
