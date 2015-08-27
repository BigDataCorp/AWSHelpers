using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Amazon;
using Amazon.CloudSearchDomain;
using Amazon.CloudSearchDomain.Model;
using Newtonsoft.Json;

namespace AWSHelpers
{
    /// <summary>
    /// This class is the wrapper that goes on top of the documents that need to be 
    /// loaded into CloudSearch
    /// </summary>
    public class AWSCloudSearchDocumentDocumentOperation
    {
        /// <summary>
        /// One of "add" or "delete"
        /// </summary>
        public string type { get; set; }

        /// <summary>
        /// Unique document id
        /// </summary>
        public string id   { get; set; }

        /// <summary>
        /// Document fields to be uploaded to CloudSearch
        /// </summary>
        public Dictionary<string, object> fields { get; set; }

        public AWSCloudSearchDocumentDocumentOperation ()
        {
            fields = new Dictionary<string, object> ();
        }

        public void AddField<T> (string fieldName, T fieldValue)
        {
            fields.Add (fieldName, fieldValue);
        }
    }

    #region *   Search-related Classes   *
    // Reference: http://docs.aws.amazon.com/cloudsearch/latest/developerguide/search-api.html#search-request-parameters
    
    /// <summary>
    /// This class holds the facet definition for the CloudSearch query string ("facet.FIELD")
    /// </summary>
    public class AWSCloudSearchFacetFieldDefinition
    {
        /// <summary>
        /// The name of the field this facet applies to
        /// </summary>
        public string FieldName { get; set; }

        /// <summary>
        /// One of "bucket" or "count"
        /// </summary>
        public string sort { get; set; }

        /// <summary>
        /// The maximum number of facets to be included in the result
        /// </summary>
        public int size { get; set; }

        /// <summary>
        /// Use this field to define the facets you want to be returned, instead of 
        /// letting CloudSearch choose them for you. To specify a range of values, use
        /// "[val1,val2]" as the bucket value. Bucket values must match the facet field values.
        /// Using this field disables the "sort" and "size" fields
        /// </summary>
        public List<string> buckets { get; set; }

        public AWSCloudSearchFacetFieldDefinition ()
        {
            buckets = new List<string> ();
        }
    }

    /// <summary>
    /// This class holds the expression definitions ("expr.NAME")
    /// </summary>
    public class AWSCloudSearchExpressionDefinition
    {
        /// <summary>
        /// The name of the expression we're adding to the result 
        /// </summary>
        public string ExpressionName { get; set; }

        /// <summary>
        /// The expression itself
        /// </summary>
        public string ExpressionValue { get; set; }
    }

    #endregion

    public class AWSCloudSearchHelper : IDisposable
    {
        private IAmazonCloudSearchDomain CloudSearchClient;

        public int    ErrorCode    { get; set; }   // Last error code
        public string ErrorMessage { get; set; }   // Last error message

        public SearchResponse LastSearchResults { get; set; }  // Last search results

        /// <summary>
        /// Class constructors: default (no parameters), with region endpoint, with endpoint + credentials and with endpoint + credentials + service endpoint
        /// </summary>
        public AWSCloudSearchHelper ()
        {
            // Set configuration info
            AmazonCloudSearchDomainConfig config = new AmazonCloudSearchDomainConfig ();
            config.Timeout                       = new TimeSpan (1, 0, 0);
            config.ReadWriteTimeout              = new TimeSpan (1, 0, 0);
            config.RegionEndpoint                = RegionEndpoint.USEast1;
            config.ServiceURL                    = Gadgets.LoadConfigurationSetting ("AWS_CloudSearch_DomainEndpoint", "");

            // Create CloudSearch client
            CloudSearchClient = new AmazonCloudSearchDomainClient (
                            Gadgets.LoadConfigurationSetting ("AWSAccessKey", ""),
                            Gadgets.LoadConfigurationSetting ("AWSSecretKey", ""),
                            config);
        }

        public AWSCloudSearchHelper (RegionEndpoint regionEndpoint)
        {
            // Set configuration info
            AmazonCloudSearchDomainConfig config = new AmazonCloudSearchDomainConfig ();
            config.Timeout                       = new TimeSpan (1, 0, 0);
            config.ReadWriteTimeout              = new TimeSpan (1, 0, 0);
            config.RegionEndpoint                = regionEndpoint;
            config.ServiceURL                    = Gadgets.LoadConfigurationSetting ("AWS_CloudSearch_DomainEndpoint", "");

            // Create CloudSearch client
            CloudSearchClient = new AmazonCloudSearchDomainClient (
                            Gadgets.LoadConfigurationSetting ("AWSAccessKey", ""),
                            Gadgets.LoadConfigurationSetting ("AWSSecretKey", ""),
                            config);
        }

        public AWSCloudSearchHelper (RegionEndpoint regionEndpoint, string AWSAcessKey, string AWSSecretKey)
        {
            // Set configuration info
            AmazonCloudSearchDomainConfig config = new AmazonCloudSearchDomainConfig ();
            config.Timeout                       = new TimeSpan (1, 0, 0);
            config.ReadWriteTimeout              = new TimeSpan (1, 0, 0);
            config.RegionEndpoint                = regionEndpoint;
            config.ServiceURL                    = Gadgets.LoadConfigurationSetting ("AWS_CloudSearch_DomainEndpoint", "");

            // Create CloudSearch client
            CloudSearchClient = new AmazonCloudSearchDomainClient (
                            AWSAcessKey,
                            AWSSecretKey,
                            config);
        }

        public AWSCloudSearchHelper (RegionEndpoint regionEndpoint, string AWSAcessKey, string AWSSecretKey, string AWSCloudSearchDomainEndpoint)
        {
            // Set configuration info
            AmazonCloudSearchDomainConfig config = new AmazonCloudSearchDomainConfig ();
            config.Timeout                       = new TimeSpan (1, 0, 0);
            config.ReadWriteTimeout              = new TimeSpan (1, 0, 0);
            config.RegionEndpoint                = regionEndpoint;
            config.ServiceURL                    = AWSCloudSearchDomainEndpoint;

            // Create CloudSearch client
            CloudSearchClient = new AmazonCloudSearchDomainClient (
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
                if (CloudSearchClient != null)
                    CloudSearchClient.Dispose ();
            }
            catch
            {
            }
            CloudSearchClient = null;
        }

        /// <summary>
        /// The method clears the error information associated with this class
        /// </summary>
        private void ClearErrorInfo ()
        {
            ErrorCode = 0;
            ErrorMessage = string.Empty;
        }

        /// <summary>
        /// This method clears the previous search results stored in this class
        /// </summary>
        private void ClearSearchResults ()
        {
            LastSearchResults = null;
        }

        /// <summary>
        /// This method uploads a set of documents onto your cloudsearch search domain. 
        /// Your objects must already have been converted to the cloudsearch upload format
        /// for document batches (
        /// </summary>
        public bool UploadDocuments (List<AWSCloudSearchDocumentDocumentOperation> documents)
        {
            ClearErrorInfo ();

            try
            {
                UploadDocumentsRequest uploadRequest = new UploadDocumentsRequest ();
                uploadRequest.ContentType            = "application/json";
                uploadRequest.Documents              = new MemoryStream(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject (documents) ?? ""));

                UploadDocumentsResponse uploadResponse = CloudSearchClient.UploadDocuments (uploadRequest);

                // Check response for errors
                if (uploadResponse.HttpStatusCode != HttpStatusCode.OK)
                {
                    ErrorCode    = Convert.ToInt32 (uploadResponse.HttpStatusCode);
                    ErrorMessage = "Http Error [" + uploadResponse.HttpStatusCode.ToString () + "]";
                }
            }
            catch (Exception ex)
            {
                ErrorCode    = -1;
                ErrorMessage = ex.Message + "::" + ex.InnerException;
            }

            return ErrorCode == 0;
        }

        /// <summary>
        /// This method runs a search query on your cloudsearch domain
        /// Ref: http://docs.aws.amazon.com/cloudsearch/latest/developerguide/search-api.html#search-request-parameters
        /// </summary>
        /// <param name="searchQuery">The search query parameters</param>
        /// <param name="filterQuery">The filter query (fq) parameters</param>
        /// <param name="cursor">Cursor to paginate results</param>
        /// <param name="parser">The type of parser to be used. "simple" and "structured" are the most common</param>
        /// <param name="size">The size of the result set to be returned</param>
        /// <param name="expressions">A list of expressions to be calculated and returned with the results</param>
        /// <param name="facets">The "facets" parameter (for data grouping)</param>
        /// <param name="returnFields">List of fields to be returned. By default, all return-enabled fields are returned.</param>
        /// <returns></returns>
        public bool RunDocumentSearch (string searchQuery, string filterQuery = "", string cursor = "", string parser = "simple", int start = 0, int size = 10, List<AWSCloudSearchExpressionDefinition> expressions = null, List<AWSCloudSearchFacetFieldDefinition> facets = null, List<string> returnFields = null)
        {
            ClearErrorInfo ();
            ClearSearchResults ();

            try
            {
                SearchRequest searchRequest = new SearchRequest ();
                searchRequest.Query         = searchQuery;
                searchRequest.QueryParser   = parser;
                searchRequest.Size          = size;                                

                // Add the parameters as long as they've been 
                if (!String.IsNullOrEmpty (filterQuery))
                {
                    searchRequest.FilterQuery = filterQuery;
                }

                if (!String.IsNullOrEmpty (cursor))
                {
                    searchRequest.Cursor = cursor;
                }
                else
                {
                    searchRequest.Start = start;
                }

                if (expressions != null && expressions.Count > 0)
                {
                    searchRequest.Expr = "{";

                    foreach (AWSCloudSearchExpressionDefinition expr in expressions)
                    {
                        searchRequest.Expr += "'" + expr.ExpressionName + "':'" + expr.ExpressionValue + "',";
                    }
                    searchRequest.Expr = searchRequest.Expr.TrimEnd (',');

                    searchRequest.Expr += "}";
                }

                if (facets != null && facets.Count > 0)
                {
                    searchRequest.Facet = "{";

                    foreach (AWSCloudSearchFacetFieldDefinition facet in facets)
                    {
                        searchRequest.Facet += "'" + facet.FieldName + "':{";
                        if (facet.buckets.Count > 0)
                        {
                            searchRequest.Facet += "buckets:['" + String.Join ("','", facet.buckets) + "']";
                        }
                        else if (!String.IsNullOrEmpty (facet.sort)) 
                        {
                            searchRequest.Facet += "sort:'" + facet.sort + "', size:" + facet.size;
                        }
                        searchRequest.Facet += "},";
                    }
                    searchRequest.Facet = searchRequest.Facet.TrimEnd (',');

                    searchRequest.Facet += "}";
                }

                if (returnFields != null && returnFields.Count > 0)
                {
                    searchRequest.Return = String.Join (",", returnFields);
                }

                SearchResponse searchResponse = CloudSearchClient.Search (searchRequest);
                // Check response for errors
                if (searchResponse.HttpStatusCode != HttpStatusCode.OK)
                {
                    ErrorCode = Convert.ToInt32 (searchResponse.HttpStatusCode);
                    ErrorMessage = "Http Error [" + searchResponse.HttpStatusCode.ToString () + "]";
                }
                else
                {
                    // Save the response on our objects
                    this.LastSearchResults = searchResponse;
                }
            }
            catch (Exception ex)
            {
                ErrorCode    = -1;
                ErrorMessage = ex.Message + "::" + ex.InnerException;
            }

            return ErrorCode == 0;
        }
    }
}
