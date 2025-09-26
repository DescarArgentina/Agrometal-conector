using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Web_Service
{
    public class Tabla_SB1
    {
        public static async Task postSB1(string jsonData)
        {
            string url = "http://119.8.73.193:8086/rest/TCProductos/Incluir/"; // URL del servicio REST
            //string url = "https://richiger-protheus-rest-val.totvs.ar/rest/TCProductos/Incluir/";
            string username = "USERREST"; // Usuario proporcionado
            string password = "restagr"; // Contraseña proporcionada
            //string username = "ADMIN"; // Usuario proporcionado
            //string password = "Totvs2024##"; // Contraseña proporcionada

            JObject obj = JObject.Parse(jsonData);
            var productos = obj["producto"];

            string codigo = null;
            string descripcion = null;
            string revision = null;

            foreach (var item in productos)
            {
                var campo = item["campo"]?.ToString();
                var valor = item["valor"]?.ToString();

                if (campo == "codigo") codigo = valor;
                if (campo == "descripcion") descripcion = valor;
                //if (campo == "revision") revision = valor;
            }

            using (HttpClient client = new HttpClient())
            {
                try
                {
                    // Configurar credenciales Basic Auth
                    var credentials = Encoding.ASCII.GetBytes($"{username}:{password}");
                    client.DefaultRequestHeaders.Authorization =
                        new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", Convert.ToBase64String(credentials));

                    // Configurar el contenido de la solicitud
                    var content = new StringContent(jsonData, Encoding.UTF8, "application/json");

                    // Realizar la solicitud POST
                    HttpResponseMessage response = await client.PostAsync(url, content);

                    if (response.StatusCode == System.Net.HttpStatusCode.Conflict)
                    {
                        Console.WriteLine("Ya existe (409). Intentando PUT /Modificar...");
                        await putSB1(jsonData);     // llamada a la función PUT
                        return;                         // salimos después del PUT
                    }

                    //// Si no fue 409, ahora sí validamos éxito del POST
                    //if (!response.IsSuccessStatusCode)
                    //{
                    //    var bodyErr = await response.Content.ReadAsStringAsync();
                    //    Console.WriteLine($"POST falló: {(int)response.StatusCode} {response.ReasonPhrase}\n{bodyErr}");
                    //    return;
                    //}

                    // Leer el código de estado
                    int statusCode = (int)response.StatusCode;

                    // Leer la respuesta como string
                    string responseData = await response.Content.ReadAsStringAsync();

                    // Mostrar el código de estado y la respuesta en consola
                    Console.WriteLine($"Código de estado: {statusCode}");
                    Console.WriteLine("Respuesta del servicio:");
                    Console.WriteLine(responseData);
                    //poblarBase(codigo, descripcion,"PA","01","UN", revision, statusCode,responseData);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error al consumir el servicio: {ex.Message}");
                }
            }
        }

        public static async Task putSB1(string jsonData)
        {
            string url = "http://119.8.73.193:8086/rest/TCProductos/Modificar/"; // URL del servicio REST
            string username = "USERREST"; // Usuario proporcionado
            string password = "restagr"; // Contraseña proporcionada

            JObject obj = JObject.Parse(jsonData);
            var productos = obj["producto"];

            string codigo = null;
            string descripcion = null;

            foreach (var item in productos)
            {
                var campo = item["campo"]?.ToString();
                var valor = item["valor"]?.ToString();

                if (campo == "codigo") codigo = valor;
                if (campo == "descripcion") descripcion = valor;
            }

            using (HttpClient client = new HttpClient())
            {
                try
                {
                    // Configurar credenciales Basic Auth
                    var credentials = Encoding.ASCII.GetBytes($"{username}:{password}");
                    client.DefaultRequestHeaders.Authorization =
                        new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", Convert.ToBase64String(credentials));

                    // Configurar el contenido de la solicitud
                    var content = new StringContent(jsonData, Encoding.UTF8, "application/json");

                    // Realizar la solicitud POST
                    HttpResponseMessage response = await client.PutAsync(url, content);

                    // Leer el código de estado
                    int statusCode = (int)response.StatusCode;

                    // Leer la respuesta como string
                    string responseData = await response.Content.ReadAsStringAsync();

                    // Mostrar el código de estado y la respuesta en consola
                    Console.WriteLine($"Código de estado: {statusCode}");
                    Console.WriteLine("Respuesta del servicio:");
                    Console.WriteLine(responseData);
                    //ActualizarBase(statusCode, responseData, codigo, descripcion);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error al consumir el servicio: {ex.Message}");
                }
            }
        }

        public static List<string> jsonSB1()
        {
            string connectionString = @"Data Source=DEPLM-11-PC\SQLEXPRESS;Initial Catalog=AgrometalBop;
                                      Integrated Security=True;TrustServerCertificate=True";

            string query = @"SELECT DISTINCT
                         Product.id_Table,
                        	 CASE WHEN LEFT(Product.productId, 2) = 'M-' THEN RIGHT(Product.productId, LEN(Product.productId) - 2)
                        	WHEN LEFT(Product.productId, 1) = 'M' THEN RIGHT(Product.productId, LEN(Product.productId) - 1)
                         WHEN LEFT(Product.productId, 1) = 'E' THEN RIGHT(Product.productId, LEN(Product.productId)-1)
                        	WHEN RIGHT(Product.productId, 3)= '-FV' THEN LEFT(Product.productId, LEN(Product.productId)-3)
                        	 ELSE Product.productId END AS codigo,
                         LEFT(pr.name,60) as Descripcion,
                        	'PA' as tipo,
                        	'01' as deposito,
                        	MAX(CASE
            	WHEN uud.title = 'Agm4_Unidad' THEN uud.value
            	WHEN uud.title = 'Agm4_Kilogramos' THEN uud.value
            	WHEN uud.title = 'Agm4_Litros' THEN uud.value
            	WHEN uud.title = 'Agm4_Metros' THEN uud.value
            	ELSE 'UN' END) AS 'unMedida',
            	pr.revision AS 'Revision',
            	CASE WHEN pr.name LIKE '%CONJ.CUBIERTAS%' OR pr.name LIKE '%GPS%' THEN
            	1 ELSE 0 END AS Fantasma
                     FROM
                         Occurrence
                     JOIN
                         ProductRevision pr ON Occurrence.instancedRef = pr.id_Table AND pr.idXml = Occurrence.idXml
                     JOIN
                         Product ON pr.masterRef = Product.id_Table AND Product.idXml = pr.idXml
                     LEFT JOIN
                         Form f ON Product.ProductId = (CASE
                             WHEN CHARINDEX('/', F.name) > 0 THEN LEFT(F.name, CHARINDEX('/', F.name) -1)
                             ELSE F.name END)
                     JOIN UserValue_UserData uud ON
            f.id_Table + 9 = uud.id_Father
            AND Occurrence.idXml = uud.idXml
                     --WHERE Product.productId = 'CDBZ50005A' OR Product.productId = 'CDBZ00004A' OR Product.productId = 'CDBZ00023A'
                     --WHERE Product.productId = 'E086129'
                     --WHERE Product.productId = '212019'
                     GROUP BY
                         Product.id_Table,Product.productId, pr.name, uud.title, Occurrence.parentRef, pr.revision";


            List<string> jsonProductos = new List<string>();

            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();

                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        command.CommandTimeout = 120;
                        using (SqlDataReader reader = command.ExecuteReader())
                        {

                            while (reader.Read())
                            {
                                string codigo = reader["codigo"].ToString();
                                string descripcion = reader["Descripcion"].ToString();
                                string tipo = reader["tipo"].ToString();
                                string deposito = reader["deposito"].ToString();
                                string unMedida = reader["unMedida"].ToString();
                                string revision = reader["Revision"].ToString();
                                //string fantasma = reader["Fantasma"].ToString();
                                // Construir el JSON para cada producto
                                var producto = new
                                {
                                    producto = new List<Dictionary<string, string>>
                                    {
                                        new Dictionary<string, string> { { "campo", "codigo" }, { "valor",  codigo} },
                                        new Dictionary<string, string> { { "campo", "descripcion" }, { "valor",  descripcion} },
                                        new Dictionary<string, string> { { "campo", "tipo" }, { "valor",  tipo} },
                                        new Dictionary<string, string> { { "campo", "deposito" }, { "valor",  deposito} },
                                        new Dictionary<string, string> { { "campo", "unMedida" }, { "valor",  unMedida} },
                                        new Dictionary<string, string> { { "campo", "revEstruct" },  {"valor", revision} },
                                        //new Dictionary<string, string> { { "campo", "fantasma"}, {"valor",  fantasma} },
                                    }
                                };

                                //poblarBase(codigo, descripcion, tipo, deposito, unMedida);
                                string jsonData = JsonConvert.SerializeObject(producto, Formatting.Indented);
                                Console.WriteLine(jsonData);
                                jsonProductos.Add(jsonData); // Guardar el JSON en la lista
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al consultar la base de datos: {ex.Message}");
            }

            return jsonProductos;
        }

        public static void poblarBase(string codigo, string descripcion, string tipo, string deposito, string unMedida, string revision, int estado, string mensaje)
        {
            string connectionString = @"Data Source=DEPLM-11-PC\SQLEXPRESS;Initial Catalog=AgrometalBop;
                                      Integrated Security=True;TrustServerCertificate=True";
            string query = "  INSERT INTO SB1 (codigo, descripcion, tipo, deposito, unMedida, revision, estado, mensaje)\r\nSELECT @codigo, @descripcion, @tipo, @deposito, @unMedida, @revision, @estado, @mensaje\r\nWHERE NOT EXISTS (SELECT 1 FROM SB1 WHERE codigo = @codigo)";
            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();

                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        command.Parameters.AddWithValue("@codigo", codigo);
                        command.Parameters.AddWithValue("@descripcion", descripcion);
                        command.Parameters.AddWithValue("@tipo", tipo);
                        command.Parameters.AddWithValue("@deposito", deposito);
                        command.Parameters.AddWithValue("@unMedida", unMedida);
                        command.Parameters.AddWithValue("@revision", revision);
                        command.Parameters.AddWithValue("@estado", estado);
                        command.Parameters.AddWithValue("@mensaje", mensaje);
                        command.ExecuteNonQuery();
                    }
                        
                }
            }
            catch (Exception ex)
            {

            }
        }

        public static void ActualizarBase(int estado, string mensaje, string codigo, string descripcion)
        {
            string connectionString = @"Data Source=DEPLM-11-PC\SQLEXPRESS;Initial Catalog=AgrometalBop;
                                      Integrated Security=True;TrustServerCertificate=True";
            string query = @"UPDATE SB1
                          SET estado = @estado, mensaje = @mensaje
                          WHERE codigo = @codigo AND descripcion = @descripcion 
--AND estado BETWEEN 400 AND 409";
            try
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    connection.Open();

                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        command.Parameters.AddWithValue("@estado", estado);
                        command.Parameters.AddWithValue("@mensaje", mensaje);
                        command.Parameters.AddWithValue("@codigo", codigo);
                        command.Parameters.AddWithValue("@descripcion", descripcion);
                        command.ExecuteNonQuery();
                    }

                }
            }
            catch (Exception ex)
            {

            }
        }
    }
}
