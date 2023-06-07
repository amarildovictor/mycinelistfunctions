using System.Data;
using System.Drawing;
using System.Drawing.Drawing2D;
using System.Drawing.Imaging;
using System.Net;
using System.Text;
using Azure.Storage.Blobs;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Extensions.Sql;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;

namespace MyCineListFunctions
{
    public class ResizingImages
    {
        private readonly ILogger _logger;

        public ResizingImages(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<ResizingImages>();
        }

        [Function("ResizingImages")]
        public static async Task<OutputType> Run([HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequestData req,
            FunctionContext executionContext)
        {
            try
            {
                var logger = executionContext.GetLogger("HttpExample");
                logger.LogInformation("C# HTTP trigger function processed a request.");

                List<ImageMovie>? imageList = new List<ImageMovie>();
                DataTable imagesTable = GetUrlImageToResize();
                
                string? Connection = Environment.GetEnvironmentVariable("AzureWebJobsStorage");
                string? containerName = Environment.GetEnvironmentVariable("ContainerName");
                var blobClient = new BlobContainerClient(Connection, containerName);

                foreach (DataRow row in imagesTable.Rows)
                {
                    string? imdbPrimaryImageUrl = row["ImdbPrimaryImageUrl"].ToString();
                    string? imdbID = row["IMDBID"].ToString();
                    
                    if (imdbPrimaryImageUrl != null && imdbID != null)
                    {
                        Bitmap? imageToResize = null;
                        
                        BlobClient blob = blobClient.GetBlobClient($"{imdbID}_{355}px.jpg" );
                        string? imageUrl355px = blob.Uri.AbsoluteUri;
                        
                        if (!blob.Exists()){
                            try
                            {
                                imageToResize = await GetImageStream(imdbPrimaryImageUrl);
                                await UploadFileToAzureStorageAccount(blob, imageToResize, imdbID, 355);
                            }
                            catch (Exception ex)
                            {
                                logger.LogInformation(ex.Message);
                                imageUrl355px = null;
                            }
                        }

                        blob = blobClient.GetBlobClient($"{imdbID}_{220}px.jpg" );
                        string? imageUrl220px = blob.Uri.AbsoluteUri;

                        if (!blob.Exists()){
                            try
                            {
                                imageToResize = imageToResize ?? await GetImageStream(imdbPrimaryImageUrl);
                                await UploadFileToAzureStorageAccount(blob, imageToResize, imdbID, 220);
                            }
                            catch (Exception ex)
                            {
                                logger.LogInformation(ex.Message);
                                imageUrl220px = null;
                            }
                        }

                        imageList.Add(ImageMovieMap(row, imageUrl355px, imageUrl220px));

                        if (imageToResize != null)
                            imageToResize.Dispose();
                    }
                }

                var message = "IMAGE_MOVIE table updated successfully!";
                var response = req.CreateResponse(HttpStatusCode.OK);
                response.Headers.Add("Content-Type", "text/plain; charset=utf-8");
                response.WriteString(message);

                return new OutputType(){
                    ImageMovieList = imageList,
                    HttpResponse = response
                };
            }
            catch (Exception ex)
            {
                var response = req.CreateResponse(HttpStatusCode.Conflict);
                response.WriteString(ex.Message);

                return new OutputType() { HttpResponse = response };
            }
        }

        private static async Task<Bitmap> GetImageStream(string? imdbPrimaryImageUrl)
        {
            try
            {
                using (var client = new HttpClient())
                {
                    var clientResponse = await client.GetAsync(imdbPrimaryImageUrl);
                    using (Stream imageStream = await clientResponse.Content.ReadAsStreamAsync())
                    {
                        imageStream.Position = 0;
                        
                        return new Bitmap(imageStream);
                    }
                }
            }
            catch (Exception ex) 
            { 
                throw new Exception($"Exception: {ex.Message} Erro ao obter a imagem: {imdbPrimaryImageUrl}", ex);
            }
        }

        private static ImageMovie ImageMovieMap(DataRow row, string? imageUrl355px, string? imageUrl220px)
        {
            return new ImageMovie() {
                ID = Convert.ToInt32(row["ID"]),
                ImdbPrimaryImageUrl = row["ImdbPrimaryImageUrl"].ToString() ?? string.Empty,
                Width = Convert.ToInt32(row["Width"]),
                Height = Convert.ToInt32(row["Height"]),
                ConsidererToResizingFunction = false,
                MediumImageUrl = imageUrl355px,
                SmallImageUrl = imageUrl220px
            };
        }

        private static DataTable GetUrlImageToResize()
        {
            using (SqlConnection conn = new SqlConnection(Environment.GetEnvironmentVariable("SqlConnectionString")))
            {
                conn.Open();
                StringBuilder query = new StringBuilder();
                query.AppendLine("select m.IMDBID, i.ID, i.ImdbPrimaryImageUrl, i.Width, i.Height from MOVIE m");
                query.AppendLine("inner join IMAGE_MOVIE i on m.ImageMovieID = i.ID");
                query.AppendLine("where i.ConsidererToResizingFunction = 1");

                SqlCommand command = new SqlCommand(query.ToString(), conn);
                command.CommandType = System.Data.CommandType.Text;

                SqlDataAdapter da = new SqlDataAdapter(command);
                DataTable dt = new DataTable();
                da.Fill(dt);

                return dt;
            }
        }

        private static async Task UploadFileToAzureStorageAccount(BlobClient blob,Bitmap actualImage, string movieId, int newWidth)
        {
            int newHeight = NewProportionalHeight(actualImage.Width, actualImage.Height, newWidth);
            Bitmap newImage = ResizeImage(actualImage, newWidth, newHeight);
        
            using (MemoryStream ms = new MemoryStream())
            {
                newImage.Save(ms, ImageFormat.Jpeg);
                
                ms.Position = 0;
                await blob.UploadAsync(ms);
            }
        }

        private static int NewProportionalHeight (int actualWidth, int actualHeight, int newWidth)
        {
            return newWidth * actualHeight / actualWidth;
        }

        /// <summary>
        /// Resize the image to the specified width and height.
        /// </summary>
        /// <param name="image">The image to resize.</param>
        /// <param name="width">The width to resize to.</param>
        /// <param name="height">The height to resize to.</param>
        /// <returns>The resized image.</returns>
        private static Bitmap ResizeImage(Image image, int width, int height)
        {
            var destRect = new Rectangle(0, 0, width, height);
            var destImage = new Bitmap(width, height);

            destImage.SetResolution(image.HorizontalResolution, image.VerticalResolution);

            using (var graphics = Graphics.FromImage(destImage))
            {
                graphics.CompositingMode = CompositingMode.SourceCopy;
                graphics.CompositingQuality = CompositingQuality.HighQuality;
                graphics.InterpolationMode = InterpolationMode.HighQualityBicubic;
                graphics.SmoothingMode = SmoothingMode.HighQuality;
                graphics.PixelOffsetMode = PixelOffsetMode.HighQuality;

                using (var wrapMode = new ImageAttributes())
                {
                    wrapMode.SetWrapMode(WrapMode.TileFlipXY);
                    graphics.DrawImage(image, destRect, 0, 0, image.Width,image.Height, GraphicsUnit.Pixel, wrapMode);
                }
            }

            return destImage;
        }
    }

    public class OutputType
    {
        [SqlOutput("dbo.IMAGE_MOVIE", connectionStringSetting: "SqlConnectionString")]
        public List<ImageMovie>? ImageMovieList { get; set; }

        public HttpResponseData? HttpResponse { get; set; }
    }

    public class ImageMovie
    {
        public ImageMovie(){ 
            ImdbPrimaryImageUrl = string.Empty;
        }

        public int ID { get; set; }
        public string ImdbPrimaryImageUrl { get; set; }
        public string? SmallImageUrl { get; set; }
        public string? MediumImageUrl { get; set; }
        public int Width { get; set; }
        public int Height { get; set; }
        public bool ConsidererToResizingFunction { get; set; }
    }
}