using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading.Tasks;
using System.Linq;
using System.Configuration;

namespace DesktopSync
{
    class Program
    {
        static string bucketName = ConfigurationManager.AppSettings["bucketName"];        
        static string basePath = ConfigurationManager.AppSettings["basePath"];
        static string indexFile = basePath + @"\indexFile.txt";
        static List<string> localFileList = new List<string>();
        static List<string> indexFileList = new List<string>();
        static List<string> deltaFileList = new List<string>();
        static string downloadBaseDir = ConfigurationManager.AppSettings["downloadBaseDir"];
        static string userName = ConfigurationManager.AppSettings["userName"];
        static string password = ConfigurationManager.AppSettings["password"];
        static string awsAccessKeyId = ConfigurationManager.AppSettings["awsAccessKeyId"];
        static string awsSecretAccessKey = ConfigurationManager.AppSettings["awsSecretAccessKey"];
        static string proxyHost = ConfigurationManager.AppSettings["proxyHost"];
        static string proxyPort = ConfigurationManager.AppSettings["proxyPort"];
        static string initSync = ConfigurationManager.AppSettings["initSync"];

        static AmazonS3Config config;
       
        
        static void Main(string[] args)
        {
            Console.WriteLine("Desktop sync");
            Console.WriteLine(bucketName);

            InitConfig();
            if (initSync == "true")
            {
                DownloadAll();
            }
            else
            {
                ScanAndUpload();
            }

            Console.WriteLine("Press any key to continue");
            Console.ReadKey();
        }
        static void InitConfig()
        {
            config = new AmazonS3Config()
            {
                ProxyCredentials = !string.IsNullOrEmpty(userName) && !string.IsNullOrEmpty(password) ? new NetworkCredential(userName, password) : null,
                ProxyHost = proxyHost,
                ProxyPort = string.IsNullOrEmpty(proxyPort) ? 0 : Int32.Parse(ConfigurationManager.AppSettings["proxyPort"]),
                RegionEndpoint = Amazon.RegionEndpoint.APSouth1
            };
        }

        static void ScanAndUpload()
        {
            try
            {                
                ScanBasePath(basePath);
                
                ScanIndexFile();
                
                GetDelta();
                
                UploadDeltaFiles();                
            }
            catch(Exception ex)
            {
                Console.Write(ex);
            }            
        }

        static void UploadDeltaFiles()
        {
            Console.WriteLine($"Uploading {deltaFileList.Count()} files");
            
            List<Task> uploadTasks = new List<Task>();
            using (var client = new AmazonS3Client(awsAccessKeyId, awsSecretAccessKey, config))
            {
                foreach (var file in deltaFileList)
                {
                    uploadTasks.Add(UploadObjectAsync(client, file).ContinueWith(x => UpdateIndexFileAsync(file)));

                }              
                Task.WaitAll(uploadTasks.ToArray());

                if (!indexFileList.Any(x => x.Contains("indexFile")) && !deltaFileList.Any(x => x.Contains("indexFile")))
                {
                    UpdateIndexFileAsync(indexFile).Wait();
                }
                UploadObjectAsync(client, indexFile).Wait();                
            }

            Console.WriteLine($"Upload completed!");
        }
        
        static void ScanBasePath(string dirPath)
        {
            DirectoryInfo directoryInfo = new DirectoryInfo(dirPath);
            localFileList.AddRange(directoryInfo.EnumerateFiles().Select(x => x.FullName));
            foreach (var directory in directoryInfo.EnumerateDirectories())
            {
                ScanBasePath(directory.FullName);                
            }
        }

        static void ScanIndexFile()
        {
            if (!File.Exists(indexFile))
                return;

            using (StreamReader reader = new StreamReader(indexFile))
            {
                while (!reader.EndOfStream)
                {
                    indexFileList.Add(reader.ReadLine());
                }
            }
        }

        static void GetDelta()
        {
            var dict = indexFileList.ToDictionary(x => x);
            foreach(var file in localFileList)
            {
                if(!dict.ContainsKey(file))
                {
                    deltaFileList.Add(file);
                }
            }
        }
        
        private static async Task UploadObjectAsync(AmazonS3Client client, string filePath)
        {
            Console.WriteLine($"Uploading {filePath}");
            var keyName = filePath.Substring(3, filePath.Length - 3);
            keyName = keyName.Replace(@"\", "/");
            // Create list to store upload part responses.
            List<UploadPartResponse> uploadResponses = new List<UploadPartResponse>();

            // Setup information required to initiate the multipart upload.
            InitiateMultipartUploadRequest initiateRequest = new InitiateMultipartUploadRequest
            {
                BucketName = bucketName,
                Key = keyName,                
            };

            // Initiate the upload.
            InitiateMultipartUploadResponse initResponse =
                await client.InitiateMultipartUploadAsync(initiateRequest);

            // Upload parts.
            long contentLength = new FileInfo(filePath).Length;
            long partSize = 5 * (long)Math.Pow(2, 20); // 5 MB

            try
            {                
                long filePosition = 0;
                for (int i = 1; filePosition < contentLength; i++)
                {
                    UploadPartRequest uploadRequest = new UploadPartRequest
                    {
                        BucketName = bucketName,
                        Key = keyName,
                        UploadId = initResponse.UploadId,
                        PartNumber = i,
                        PartSize = partSize,
                        FilePosition = filePosition,
                        FilePath = filePath                        
                    };

                    // Track upload progress.
                    uploadRequest.StreamTransferProgress +=
                        new EventHandler<StreamTransferProgressArgs>(UploadPartProgressEventCallback);

                    // Upload a part and add the response to our list.
                    uploadResponses.Add(await client.UploadPartAsync(uploadRequest));

                    filePosition += partSize;
                }

                // Setup to complete the upload.
                CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest
                {
                    BucketName = bucketName,
                    Key = keyName,
                    UploadId = initResponse.UploadId                    
                };
                completeRequest.AddPartETags(uploadResponses);

                // Complete the upload.
                CompleteMultipartUploadResponse completeUploadResponse =
                    await client.CompleteMultipartUploadAsync(completeRequest);
            }
            catch (Exception exception)
            {
                Console.WriteLine("An AmazonS3Exception was thrown: { 0}", exception.Message);

                // Abort the upload.
                AbortMultipartUploadRequest abortMPURequest = new AbortMultipartUploadRequest
                {
                    BucketName = bucketName,
                    Key = keyName,
                    UploadId = initResponse.UploadId
                };
                await client.AbortMultipartUploadAsync(abortMPURequest);
            }
        }

        public static void UploadPartProgressEventCallback(object sender, StreamTransferProgressArgs e)
        {
            // Process event. 
            //Console.WriteLine("{0}/{1}", e.TransferredBytes, e.TotalBytes);
        }

        private static async Task UpdateIndexFileAsync(string filename)
        {
            using (StreamWriter writer = new StreamWriter(indexFile, true))
            {
                await writer.WriteLineAsync(filename);
            }
        }

        static async Task<ListBucketsResponse> ListBuckets()
        {
            using (var client = new AmazonS3Client(awsAccessKeyId, awsSecretAccessKey, config))
                return await client.ListBucketsAsync();
        }

        static async Task<ListObjectsResponse> ListBucketItems()
        {
            using (var client = new AmazonS3Client(awsAccessKeyId, awsSecretAccessKey, config))
                return await client.ListObjectsAsync(bucketName);
        }

        static void DownloadAll()
        {            
            ListObjectsResponse response = ListBucketItems().Result;

            foreach(var obj in response.S3Objects)
            {
                
                string key = obj.Key;                

                string fullFilePath = downloadBaseDir  + key.Replace("/", @"\").Replace(basePath.Substring(3, basePath.Length - 3), string.Empty);
                //fullFilePath = fullFilePath.Replace("/", @"\");

                Console.WriteLine($"Downloading {fullFilePath}...");
                
                DirectoryInfo directoryInfo = new DirectoryInfo(Path.GetDirectoryName(fullFilePath));
                if (!directoryInfo.Exists)
                {
                    directoryInfo.Create();
                }                
                ReadObjectDataAsync(key, fullFilePath).Wait();                
            }
            Console.WriteLine("Download Complete!");
        }
        
        static async Task ReadObjectDataAsync(string keyName, string fullFilePath)
        {            
            try
            {
                GetObjectRequest request = new GetObjectRequest
                {
                    BucketName = bucketName,
                    Key = keyName
                };

                using (var client = new AmazonS3Client(awsAccessKeyId, awsSecretAccessKey, config))
                using (GetObjectResponse response = await client.GetObjectAsync(request))
                using (Stream responseStream = response.ResponseStream)
                {
                    using (BinaryWriter DestinationWriter = new BinaryWriter(new FileStream(fullFilePath, FileMode.Create)))
                    {
                        responseStream.CopyTo(DestinationWriter.BaseStream);
                    }
                }                
            }
            catch (AmazonS3Exception e)
            {
                Console.WriteLine("Error encountered ***. Message:'{0}' when writing an object", e.Message);
            }
            catch (Exception e)
            {
                Console.WriteLine("Unknown encountered on server. Message:'{0}' when writing an object", e.Message);
            }
            
        }

        static void CopyFilesAsync(BinaryReader Source, BinaryWriter Destination)
        {
            char[] buffer = new char[0x1000];
            
            while (Source.BaseStream.Position != Source.BaseStream.Length)
            {
                buffer = Source.ReadChars(buffer.Length);
                Destination.Write(buffer);
            }                
        }
    }
}
