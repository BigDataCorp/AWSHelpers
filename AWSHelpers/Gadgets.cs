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
using System.Configuration;
using System.Linq;
using System.Threading;

namespace AWSHelpers
{
    class Gadgets
    {
        #region app.confile file access
        public static string LoadConfigurationSetting(string keyname, string defaultvalue)
        {
            string result = defaultvalue;
            try
            {
                result = ConfigurationManager.AppSettings[keyname];
            }
            catch
            {
                result = defaultvalue;
            }
            if (result == null)
                result = defaultvalue;
            return result;
        }
        #endregion

        #region *   Base-64 Encode / Decode   *
        public static string Base64Encode (string plainText)
        {
            var plainTextBytes = System.Text.Encoding.UTF8.GetBytes (plainText);
            return System.Convert.ToBase64String (plainTextBytes);
        }

        public static string Base64Decode (string base64EncodedData)
        {
            var base64EncodedBytes = System.Convert.FromBase64String (base64EncodedData);
            return System.Text.Encoding.UTF8.GetString (base64EncodedBytes);
        }
        #endregion

        #region Random Generator
        [ThreadStatic]
        private static Random localrandomgenerator;

        public static Random ThreadRandomGenerator()
        {
            if (localrandomgenerator == null)
                localrandomgenerator = new Random(unchecked(Environment.TickCount * 31 + Thread.CurrentThread.ManagedThreadId));
            return localrandomgenerator;
        }
        #endregion
    }
}
