using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using Amazon;
using Amazon.S3;
using Amazon.S3.IO;
using Amazon.S3.Model;
using Amazon.S3.Transfer;

namespace AWSHelpers
{
    public class AWSS3Helper : IDisposable
    {
        ///////////////////////////////////////////////////////////////////////
        //                           Fields                                  //
        ///////////////////////////////////////////////////////////////////////

        private IAmazonS3 S3client;
        private TransferUtility fileTransferUtility;

        public int ErrorCode                            { get; set; }   // Last error code
        public string ErrorMessage                      { get; set; }   // Last error message

        public delegate void ProgressNotify(int items, out bool abort);

        public const int e_Exception = -1;

        public const int loadstrings_None                       = 0;
        public const int loadstrings_TextWithoutSpecials        = 1;
        public const int loadstrings_HTMLTextDecodeAndUnescape  = 2;

        ///////////////////////////////////////////////////////////////////////
        //                    Methods & Functions                            //
        ///////////////////////////////////////////////////////////////////////

        /// <summary>
        /// Class constructor
        /// </summary>
        public AWSS3Helper(RegionEndpoint regionendpoint)
        {
            // Set configuration info
            AmazonS3Config config = new AmazonS3Config();
            config.Timeout = new TimeSpan(1, 0, 0);
            config.ReadWriteTimeout = new TimeSpan(1, 0, 0);
            config.RegionEndpoint = regionendpoint;

            // Create S3 client
            S3client = Amazon.AWSClientFactory.CreateAmazonS3Client
                        (Gadgets.LoadConfigurationSetting("AWSAccessKey", ""),
                         Gadgets.LoadConfigurationSetting("AWSSecretKey", ""),
                         config);

            // Create the file transfer utility class
            fileTransferUtility = new TransferUtility(S3client);
        }

        /// <summary>
        /// Class disposer
        /// </summary>
        public void Dispose()
        {
            try
            {
                if (S3client != null)
                    S3client.Dispose();
            }
            catch
            {
            }
            S3client = null;
        }

        /// <summary>
        /// The method clears the error information associated with this class
        /// </summary>
        private void ClearErrorInfo()
        {
            ErrorCode = 0;
            ErrorMessage = string.Empty;
        }

        /// <summary>
        /// The method saves the string "datavalue" associated with a key "dataname" into the bucket "bucketname"
        /// </summary>
        public bool SaveString(string bucketname, string dataname, string datavalue)
        {
            // Reset error info
            ClearErrorInfo();

            // Save data
            try
            {
                PutObjectRequest request = new PutObjectRequest();
                request.BucketName = bucketname;
                request.Key = dataname;
                request.ContentType = "text/plain";
                request.ContentBody = datavalue;
                request.CannedACL = S3CannedACL.PublicReadWrite;
                request.StorageClass = S3StorageClass.ReducedRedundancy; 
                S3client.PutObject(request);
            }
            catch (Exception ex)
            {
                ErrorCode = e_Exception;
                ErrorMessage = ex.Message + "::" + ex.InnerException;
            }

            return ErrorCode == 0;
        }

        /// <summary>
        /// Saves the string "datavalue" associated with a key "dataname" into the bucket "bucketname"
        /// </summary>
        public bool SaveString(string bucketname, string dataname, string datavalue, int maxretries)
        {
            // Save data
            bool result = false;
            int retrycount = maxretries;
            while (true)
            {
                // Try the insertion
                if (SaveString(bucketname, dataname, datavalue))
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
        /// The method saves the byte array "datavalue" associated with a key "dataname" 
        /// into the bucket "bucketname"
        /// </summary>
        public bool SaveByteArray(string bucketname, string dataname, byte[] datavalue)
        {
            // Reset error info
            ClearErrorInfo();

            // Save data
            try
            {
                PutObjectRequest request = new PutObjectRequest();
                request.BucketName = bucketname;
                request.Key = dataname;
                request.ContentType = "binary/octet-stream";
                request.CannedACL = S3CannedACL.PublicReadWrite;
                request.StorageClass = S3StorageClass.ReducedRedundancy;
                using (MemoryStream stream = new MemoryStream(datavalue))
                {
                    request.InputStream = stream;
                    S3client.PutObject(request);
                }
            }
            catch (Exception ex)
            {
                ErrorCode = e_Exception;
                ErrorMessage = ex.Message + "::" + ex.InnerException;
            }

            return ErrorCode == 0;
        }

        /// <summary>
        /// Saves the byte array "datavalue" associated with a key "dataname" into the bucket "bucketname"
        /// </summary>
        public bool SaveByteArray(string bucketname, string dataname, byte[] datavalue, int maxretries)
        {
            // Save data
            bool result = false;
            int retrycount = maxretries;
            while (true)
            {
                // Try the insertion
                if (SaveByteArray(bucketname, dataname, datavalue))
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
        /// Saves a file to S3.
        /// </summary>
        public bool SaveFile(string bucketname, string dataname, string filepath)
        {
            // Reset error info
            ClearErrorInfo();

            // Save data
            try
            {
                PutObjectRequest request = new PutObjectRequest();
                request.BucketName = bucketname;
                request.Key = dataname;
                request.ContentType = "binary/octet-stream";
                request.CannedACL = S3CannedACL.PublicReadWrite;
                request.StorageClass = S3StorageClass.ReducedRedundancy;
                using (FileStream stream = new FileStream(filepath, FileMode.Open, FileAccess.Read))
                {
                    request.InputStream = stream;
                    S3client.PutObject(request);
                }
            }
            catch (Exception ex)
            {
                ErrorCode = e_Exception;
                ErrorMessage = ex.Message + "::" + ex.InnerException;
            }

            return ErrorCode == 0;
        }

        /// <summary>
        /// The method loads the string "datavalue" associated with a key "dataname" from the bucket "bucketname"
        /// </summary>
        public bool LoadString(string bucketname, string dataname, out string datavalue)
        {
            // Reset error info
            ClearErrorInfo();

            // Reset datavalue
            datavalue = string.Empty;

            // Load data
            try
            {
                // Send the request
                GetObjectRequest request = new GetObjectRequest();
                request.BucketName = bucketname;
                request.Key = dataname;
                GetObjectResponse response = S3client.GetObject(request);

                // Get the response
                string row;
                StringBuilder sb = new StringBuilder();
                using (Stream amazonStream = response.ResponseStream)
                {
                    StreamReader amazonStreamReader = new StreamReader(amazonStream);
                    while ((row = amazonStreamReader.ReadLine()) != null)
                    {
                        if (!string.IsNullOrWhiteSpace(row))
                            sb.Append(row);
                    }
                }

                // Save the response into datavalue
                datavalue = sb.ToString();
            }
            catch (Exception ex)
            {
                ErrorCode = e_Exception;
                ErrorMessage = ex.Message + "::" + ex.InnerException;
            }

            return ErrorCode == 0;
        }

        /// <summary>
        /// The method loads the HashSet associated with a key "dataname" from the bucket "bucketname"
        /// </summary>
        public bool LoadStrings(string bucketname, string dataname, HashSet<string> hs, int option = loadstrings_TextWithoutSpecials)
        {
            // Reset error info
            ClearErrorInfo();

            // Load data
            try
            {
                // Send the request
                GetObjectRequest request = new GetObjectRequest();
                request.BucketName = bucketname;
                request.Key = dataname;
                GetObjectResponse response = S3client.GetObject(request);

                // Get the response
                string row;
                using (Stream amazonStream = response.ResponseStream)
                {
                    StreamReader amazonStreamReader = new StreamReader(amazonStream);
                    while ((row = amazonStreamReader.ReadLine()) != null)
                    {
                        // Any special processing?
                        switch (option)
                        {
                            case loadstrings_TextWithoutSpecials:
                                row = TextTransforms.TextWithoutSpecials(row).Trim();
                                break;
                            case loadstrings_HTMLTextDecodeAndUnescape:
                                row = TextTransforms.HTMLTextDecodeAndUnescape(row).Trim();
                                break;
                        }
                        if (string.IsNullOrWhiteSpace(row))
                            continue;

                        // Save data
                        if (!string.IsNullOrWhiteSpace(row))
                            if (!hs.Contains(row))
                                hs.Add(row);
                    }
                }
            }
            catch (Exception ex)
            {
                ErrorCode = e_Exception;
                ErrorMessage = ex.Message + "::" + ex.InnerException;
            }

            return ErrorCode == 0;
        }

        /// <summary>
        /// The method loads the list associated with a key "dataname" from the bucket "bucketname"
        /// </summary>
        public bool LoadStrings(string bucketname, string dataname, List<string> alist, int option = loadstrings_TextWithoutSpecials)
        {
            // Reset error info
            ClearErrorInfo();

            // Load data
            try
            {
                // Send the request
                GetObjectRequest request = new GetObjectRequest();
                request.BucketName = bucketname;
                request.Key = dataname;
                GetObjectResponse response = S3client.GetObject(request);

                // Get the response
                string row;
                using (Stream amazonStream = response.ResponseStream)
                {
                    StreamReader amazonStreamReader = new StreamReader(amazonStream);
                    while ((row = amazonStreamReader.ReadLine()) != null)
                    {
                        // Any special processing?
                        switch (option)
                        {
                            case loadstrings_TextWithoutSpecials:
                                row = TextTransforms.TextWithoutSpecials(row).Trim();
                                break;
                            case loadstrings_HTMLTextDecodeAndUnescape:
                                row = TextTransforms.HTMLTextDecodeAndUnescape(row).Trim();
                                break;
                        }
                        if (string.IsNullOrWhiteSpace(row))
                            continue;

                        // Save data
                        alist.Add(row);
                    }
                }
            }
            catch (Exception ex)
            {
                ErrorCode = -1;
                ErrorMessage = ex.Message + "::" + ex.InnerException;
            }

            return ErrorCode == 0;
        }

        /// <summary>
        /// The method loads the byte array "datavalue" associated with a key "dataname" from the bucket "bucketname"
        /// </summary>
        public bool LoadByteArray(string bucketname, string dataname, out byte[] datavalue)
        {
            // Reset error info
            ClearErrorInfo();

            // Reset datavalue
            datavalue = null;

            // Load data
            try
            {
                // Send the request
                GetObjectRequest request = new GetObjectRequest();
                request.BucketName = bucketname;
                request.Key = dataname;
                GetObjectResponse response = S3client.GetObject(request);

                // Prepare datavalue
                datavalue = new byte[response.ContentLength];

                // Get the response
                using (Stream amazonStream = response.ResponseStream)
                using (BinaryReader reader = new BinaryReader(amazonStream))
                {
                    int bytesread;
                    int bytesleft = datavalue.Length;
                    int datavalueix = 0;
                    while (bytesleft > 0)
                    {
                        bytesread = reader.Read(datavalue, datavalueix, bytesleft);
                        if (bytesread == 0)
                            break;
                        else
                        {
                            datavalueix += bytesread;
                            bytesleft -= bytesread;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                ErrorCode = e_Exception;
                ErrorMessage = ex.Message + "::" + ex.InnerException;
            }

            return ErrorCode == 0;
        }

        /// <summary>
        /// The method loads the byte array "datavalue" associated with a key "dataname" from the bucket "bucketname"
        /// </summary>
        public bool LoadByteArray(string bucketname, string dataname, out byte[] datavalue, int maxretries)
        {
            // Initialize results
            bool result = false;
            datavalue = null;

            // Load info
            int retrycount = maxretries;
            while (true)
            {
                // Try to load info
                if (LoadByteArray(bucketname, dataname, out datavalue))
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
        /// Transfers a file to S3. This method uses the "TransferUtility" class in order to upload the file to S3
        /// </summary>
        public bool FileUpload(string bucketname, string dataname, string filepath)
        {
            // Reset error info
            ClearErrorInfo();

            // Save data
            try
            {
                TransferUtilityUploadRequest fileTransferUtilityRequest = new TransferUtilityUploadRequest
                {
                    BucketName      = bucketname,
                    FilePath        = filepath,
                    StorageClass    = S3StorageClass.ReducedRedundancy,
                    PartSize        = 6291456, // 6 MB.
                    Key             = dataname,
                    ContentType     = "binary/octet-stream",
                    CannedACL       = S3CannedACL.PublicReadWrite
                };
                fileTransferUtility.Upload(fileTransferUtilityRequest);
            }
            catch (Exception ex)
            {
                ErrorCode = e_Exception;
                ErrorMessage = ex.Message + "::" + ex.InnerException;
            }

            return ErrorCode == 0;
        }

        /// <summary>
        /// Transfers a file from S3. This method uses the "TransferUtility" class in order to download the file from S3
        /// </summary>
        public bool FileDownload(string bucketname, string dataname, string filepath)
        {
            // Reset error info
            ClearErrorInfo();

            // Load data
            try
            {
                TransferUtilityDownloadRequest fileTransferUtilityRequest = new TransferUtilityDownloadRequest
                {
                    BucketName      = bucketname,
                    FilePath        = filepath,
                    Key             = dataname,
                };
                fileTransferUtility.Download(fileTransferUtilityRequest);
            }
            catch (Exception ex)
            {
                ErrorCode = e_Exception;
                ErrorMessage = ex.Message + "::" + ex.InnerException;
            }

            return ErrorCode == 0;
        }

        /// <summary>
        /// Deletes "dataname" from the bucket "bucketname"
        /// </summary>
        public bool DeleteDataItem(string bucketname, string dataname)
        {
            // Reset error info
            ClearErrorInfo();

            // Delete data
            try
            {
                DeleteObjectRequest request = new DeleteObjectRequest();
                request.BucketName = bucketname;
                request.Key = dataname;
                S3client.DeleteObject(request);
            }
            catch (Exception ex)
            {
                ErrorCode = -1;
                ErrorMessage = ex.Message;
            }

            return ErrorCode == 0;
        }

        /// <summary>
        /// The method returns true if the "dataname" item exists
        /// </summary>
        public bool DataItemExists(string bucketname, string dataname)
        {
            // Reset error info
            ClearErrorInfo();

            // Data exists
            bool result = false;
            try
            {
                S3FileInfo s3FileInfo = new Amazon.S3.IO.S3FileInfo(S3client, bucketname, dataname);
                result = s3FileInfo.Exists;
            }
            catch (Exception ex)
            {
                ErrorCode = -1;
                ErrorMessage = ex.Message;
            }

            return result;
        }

        /// <summary>
        /// The method returns a list with all data items stored in the bucket "bucketname"
        /// </summary>
        public void ListDataItems(string bucketname, List<string> dataitems, ProgressNotify pNotify, int maxitems = 0)
        {
            try
            {
                ListObjectsResponse response;
                ListObjectsRequest request = new ListObjectsRequest();
                request.BucketName = bucketname;

                if (maxitems == 0)
                    maxitems = int.MaxValue;

                bool abort = false;
                while (true)
                {
                    // Get the partial list
                    response = S3client.ListObjects(request);

                    // Process response
                    foreach (S3Object entry in response.S3Objects)
                        dataitems.Add(entry.Key);

                    // Notify
                    if (pNotify != null)
                    {
                        pNotify(dataitems.Count, out abort);
                        if (abort)
                            break;
                    }

                    // If response is truncated, set the marker to get the next set of keys
                    if (response.IsTruncated)
                        request.Marker = response.NextMarker;
                    else
                        break;

                    // Limit reached
                    if (dataitems.Count >= maxitems)
                        break;
                }
            }
            catch (Exception ex)
            {
                ErrorCode = -1;
                ErrorMessage = ex.Message;
            }
        }

        /// <summary>
        /// The method returns a list with all data items stored in the bucket "bucketname"
        /// </summary>
        public void ListDataItems(string bucketname, List<string> dataitems, string mask, ProgressNotify pNotify, int maxitems = 0)
        {
            try
            {
                ListObjectsResponse response;
                ListObjectsRequest request = new ListObjectsRequest();
                request.BucketName = bucketname;

                if (maxitems == 0)
                    maxitems = int.MaxValue;

                bool abort = false;
                while (true)
                {
                    // Get the partial list
                    response = S3client.ListObjects(request);

                    // Process response
                    foreach (S3Object entry in response.S3Objects)
                    {
                        if (entry.Key.IndexOf(mask, StringComparison.OrdinalIgnoreCase) > -1)
                            dataitems.Add(entry.Key);
                    }

                    // Notify
                    if (pNotify != null)
                    {
                        pNotify(dataitems.Count, out abort);
                        if (abort)
                            break;
                    }

                    // If response is truncated, set the marker to get the next set of keys
                    if (response.IsTruncated)
                        request.Marker = response.NextMarker;
                    else
                        break;

                    // Limit reached
                    if (dataitems.Count >= maxitems)
                        break;
                }
            }
            catch (Exception ex)
            {
                ErrorCode = -1;
                ErrorMessage = ex.Message;
            }
        }
    }
}
