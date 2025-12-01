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
        //public static async Task postSB1(string jsonData)
        //{
        //    string url = "http://119.8.73.193:8086/rest/TCProductos/Incluir/"; // URL del servicio REST
        //    //string url = "https://richiger-protheus-rest-val.totvs.ar/rest/TCProductos/Incluir/";
        //    string username = "USERREST"; // Usuario proporcionado
        //    string password = "restagr"; // Contraseña proporcionada
        //    //string username = "ADMIN"; // Usuario proporcionado
        //    //string password = "Totvs2024##"; // Contraseña proporcionada

        //    JObject obj = JObject.Parse(jsonData);
        //    var productos = obj["producto"];

        //    string codigo = null;
        //    string descripcion = null;
        //    string revision = null;

        //    foreach (var item in productos)
        //    {
        //        var campo = item["campo"]?.ToString();
        //        var valor = item["valor"]?.ToString();

        //        if (campo == "codigo") codigo = valor;
        //        if (campo == "descripcion") descripcion = valor;
        //        //if (campo == "revision") revision = valor;
        //    }

        //    using (HttpClient client = new HttpClient())
        //    {
        //        try
        //        {
        //            // Configurar credenciales Basic Auth
        //            var credentials = Encoding.ASCII.GetBytes($"{username}:{password}");
        //            client.DefaultRequestHeaders.Authorization =
        //                new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", Convert.ToBase64String(credentials));

        //            // Configurar el contenido de la solicitud
        //            var content = new StringContent(jsonData, Encoding.UTF8, "application/json");

        //            // Realizar la solicitud POST
        //            HttpResponseMessage response = await client.PostAsync(url, content);

        //            if (response.StatusCode == System.Net.HttpStatusCode.Conflict)
        //            {
        //                Console.WriteLine("Ya existe (409). Intentando PUT /Modificar...");
        //                await putSB1(jsonData);     // llamada a la función PUT
        //                return;                         // salimos después del PUT
        //            }

        //            //// Si no fue 409, ahora sí validamos éxito del POST
        //            //if (!response.IsSuccessStatusCode)
        //            //{
        //            //    var bodyErr = await response.Content.ReadAsStringAsync();
        //            //    Console.WriteLine($"POST falló: {(int)response.StatusCode} {response.ReasonPhrase}\n{bodyErr}");
        //            //    return;
        //            //}

        //            // Leer el código de estado
        //            int statusCode = (int)response.StatusCode;

        //            // Leer la respuesta como string
        //            string responseData = await response.Content.ReadAsStringAsync();

        //            // Mostrar el código de estado y la respuesta en consola
        //            Console.WriteLine($"Código de estado: {statusCode}");
        //            Console.WriteLine("Respuesta del servicio:");
        //            Console.WriteLine(responseData);
        //            poblarBase(codigo, descripcion,"PA","01","UN", revision, statusCode,responseData);
        //        }
        //        catch (Exception ex)
        //        {
        //            Console.WriteLine($"Error al consumir el servicio: {ex.Message}");
        //        }
        //    }
        //}
        public static async Task postSB1(string jsonData)
        {
            string url = "http://119.8.73.193:8086/rest/TCProductos/Incluir/";
            string username = "USERREST";
            string password = "restagr";

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

            // 🔎 LOG CLAVE
            Console.WriteLine($"[SB1] Enviando producto a Totvs -> codigo: {codigo}, descripcion: {descripcion}");

            using (HttpClient client = new HttpClient())
            {
                try
                {
                    var credentials = Encoding.ASCII.GetBytes($"{username}:{password}");
                    client.DefaultRequestHeaders.Authorization =
                        new System.Net.Http.Headers.AuthenticationHeaderValue("Basic", Convert.ToBase64String(credentials));

                    var content = new StringContent(jsonData, Encoding.UTF8, "application/json");

                    HttpResponseMessage response = await client.PostAsync(url, content);

                    if (response.StatusCode == System.Net.HttpStatusCode.Conflict)
                    {
                        Console.WriteLine("Ya existe (409). Intentando PUT /Modificar...");
                        await putSB1(jsonData);
                        return;
                    }

                    int statusCode = (int)response.StatusCode;
                    string responseData = await response.Content.ReadAsStringAsync();

                    Console.WriteLine($"Código de estado: {statusCode}");
                    Console.WriteLine("Respuesta del servicio:");
                    Console.WriteLine(responseData);

                    poblarBase(codigo, descripcion, "PA", "01", "UN", revision, statusCode, responseData);
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

                    // Realizar la solicitud PUT
                    HttpResponseMessage response = await client.PutAsync(url, content);

                    // Leer el código de estado
                    int statusCode = (int)response.StatusCode;

                    // Leer la respuesta como string
                    string responseData = await response.Content.ReadAsStringAsync();

                    // Mostrar el código de estado y la respuesta en consola
                    Console.WriteLine($"Código de estado: {statusCode}");
                    Console.WriteLine("Respuesta del servicio:");
                    Console.WriteLine(responseData);
                    ActualizarBase(statusCode, responseData, codigo, descripcion);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error al consumir el servicio: {ex.Message}");
                }
            }
        }
        public static List<string> jsonSB1_BOP()
        {
            string connectionString = @"Data Source=DEPLM-11-PC\SQLEXPRESS;Initial Catalog=AgrometalBop;
                                Integrated Security=True;TrustServerCertificate=True";

            string query = @"
WITH CTE_Hierarchy AS (
    SELECT 
        o.id_Table,
        pr.name,
        CASE WHEN LEFT(p.catalogueId, 2) = 'P-' 
             THEN RIGHT(p.catalogueId, LEN(p.catalogueId) - 2) 
             ELSE p.catalogueId 
        END AS catalogueId,
        CAST(o.parentRef AS INT) AS parentRef,
        pr.revision,
        pr.subType,
        o.idXml,

        -- 👇 columnas alineadas con SB1
        'PA' AS Tipo,
        '01' AS Deposito,
        CASE 
            WHEN uudUnidad.title = 'Agm4_Unidad'     THEN uudUnidad.value
            WHEN uudUnidad.title = 'Agm4_Kilogramos' THEN uudUnidad.value
            WHEN uudUnidad.title = 'Agm4_Litros'     THEN uudUnidad.value
            WHEN uudUnidad.title = 'Agm4_Metros'     THEN uudUnidad.value
            ELSE 'UN'
        END AS unMedida

    FROM ProcessOccurrence o
    INNER JOIN ProcessRevision pr ON o.instancedRef = pr.id_Table
    INNER JOIN Process p          ON pr.masterRef   = p.id_Table
    LEFT JOIN Form fUnidad
           ON p.catalogueId = CASE
                                WHEN CHARINDEX('/', fUnidad.name) > 0 
                                     THEN LEFT(fUnidad.name, CHARINDEX('/', fUnidad.name) - 1)
                                ELSE fUnidad.name
                             END
    LEFT JOIN UserValue_UserData uudUnidad
           ON fUnidad.id_Table + 9 = uudUnidad.id_Father
          AND p.idXml              = uudUnidad.idXml
),

CTE_Niveles AS (
    -- Nivel 0 → nodos raíz
    SELECT 
        h.*,
        0 AS Nivel
    FROM CTE_Hierarchy h
    WHERE h.parentRef IS NULL OR h.parentRef = 0

    UNION ALL

    -- Niveles siguientes (hijos)
    SELECT 
        h.*,
        n.Nivel + 1
    FROM CTE_Hierarchy h
    INNER JOIN CTE_Niveles n ON h.parentRef = n.id_Table
)

-- 🟢 PRIMERA PARTE: jerarquía
SELECT
    COALESCE(Parent.name, '')        AS Nombre_Padre,
    COALESCE(Parent.catalogueId, '') AS Codigo_Padre,
    Child.name                       AS Nombre_Hijo,
    Child.catalogueId                AS Codigo_Hijo,
    Child.subType                    AS Subtype_Hijo,
    Child.revision                   AS Revision,
    1                                AS CantidadHijo_Total,
    nChild.Nivel                     AS Nivel,
    Child.Tipo,
    Child.Deposito,
    Child.unMedida
FROM CTE_Niveles nChild
LEFT JOIN CTE_Niveles   nParent ON nChild.parentRef = nParent.id_Table
LEFT JOIN CTE_Hierarchy Parent  ON nParent.id_Table = Parent.id_Table
LEFT JOIN CTE_Hierarchy Child   ON nChild.id_Table  = Child.id_Table

UNION ALL

-- 🟠 SEGUNDA PARTE: MEConsumed (hojas) con la MISMA lógica de unidad
SELECT 
    Operation.name                      AS Nombre_Padre,
    p2.catalogueId                      AS Codigo_Padre,
    p.name                              AS Nombre_Hijo,
    CASE WHEN LEFT(productId, 1) = 'E' 
         THEN RIGHT(productId, LEN(productId) - 1) 
         ELSE productId 
    END                                 AS Codigo_Hijo,
    pr.subType                          AS Subtype_Hijo,
    pr.revision                         AS Revision,
    COUNT(productId)                    AS CantidadHijo_Total,
    3                                   AS Nivel,
    'PA'                                AS Tipo,
    '01'                                AS Deposito,
    -- 👇 lógica de unidad de medida aplicada también acá
    MAX(
        CASE 
            WHEN uudUnidad2.title = 'Agm4_Unidad'     THEN uudUnidad2.value
            WHEN uudUnidad2.title = 'Agm4_Kilogramos' THEN uudUnidad2.value
            WHEN uudUnidad2.title = 'Agm4_Litros'     THEN uudUnidad2.value
            WHEN uudUnidad2.title = 'Agm4_Metros'     THEN uudUnidad2.value
            ELSE 'UN'
        END
    )                                   AS unMedida

FROM Occurrence
INNER JOIN ProductRevision pr ON pr.id_Table = Occurrence.instancedRef
INNER JOIN Product         p  ON p.id_Table  = pr.masterRef
LEFT JOIN ProcessOccurrence o   ON Occurrence.parentRef = o.id_Table
LEFT JOIN OperationRevision op  ON op.id_Table         = o.instancedRef
LEFT JOIN Operation             ON Operation.id_Table  = op.masterRef
LEFT JOIN ProcessOccurrence o2  ON o2.id_Table         = o.parentRef
LEFT JOIN ProcessRevision pr2   ON o2.instancedRef     = pr2.id_Table
LEFT JOIN Process p2            ON pr2.masterRef       = p2.id_Table

-- 👇 joins para unidad de medida en la rama MEConsumed
LEFT JOIN Form fUnidad2
       ON p2.catalogueId = CASE
                             WHEN CHARINDEX('/', fUnidad2.name) > 0 
                                  THEN LEFT(fUnidad2.name, CHARINDEX('/', fUnidad2.name) - 1)
                             ELSE fUnidad2.name
                          END
LEFT JOIN UserValue_UserData uudUnidad2
       ON fUnidad2.id_Table + 9 = uudUnidad2.id_Father
      AND p2.idXml              = uudUnidad2.idXml

WHERE Occurrence.subType = 'MEConsumed'
GROUP BY 
    Operation.name,
    p2.catalogueId,
    p.name,
    productId,
    pr.subType,
    pr.revision
ORDER BY 
    Nivel,
    Codigo_Hijo DESC;
    ";

            // 🔑 Diccionario para deduplicar por código
            var productosDict = new Dictionary<string, string>(); // codigo -> jsonData
            int totalFilasSql = 0;

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
                                totalFilasSql++;

                                string codigo = reader["Codigo_Hijo"].ToString();
                                string descripcion = reader["Nombre_Hijo"].ToString();
                                string tipo = reader["Tipo"].ToString();
                                string deposito = reader["Deposito"].ToString();
                                string unMedida = reader["unMedida"].ToString();
                                string revision = reader["Revision"].ToString();

                                Console.WriteLine(
                                    $"[SB1 SQL] codigo_hijo={codigo} | desc={descripcion} | tipo={tipo} | dep={deposito} | UM={unMedida} | rev={revision}"
                                );

                                var producto = new
                                {
                                    producto = new List<Dictionary<string, string>>
        {
            new() { { "campo", "codigo"      }, { "valor", codigo      } },
            new() { { "campo", "descripcion" }, { "valor", descripcion } },
            new() { { "campo", "tipo"        }, { "valor", tipo        } },
            new() { { "campo", "deposito"    }, { "valor", deposito    } },
            new() { { "campo", "unMedida"    }, { "valor", unMedida    } },
            new() { { "campo", "revEstruct"  }, { "valor", revision    } },
        }
                                };

                                string jsonData = JsonConvert.SerializeObject(producto, Formatting.Indented);

                                // log de deduplicación
                                if (productosDict.ContainsKey(codigo))
                                {
                                    Console.WriteLine($"[SB1] Código {codigo} ya existía, se reemplaza el JSON anterior por el nuevo.");
                                }
                                else
                                {
                                    Console.WriteLine($"[SB1] Nuevo código agregado al diccionario: {codigo}");
                                }

                                productosDict[codigo] = jsonData;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al consultar la base de datos: {ex.Message}");
            }

            //var jsonProductos = new List<string>();
            //foreach (var kvp in productosDict)
            //{
            //    Console.WriteLine("-- sb1 unico --");
            //    Console.WriteLine(kvp.Value);
            //    jsonProductos.Add(kvp.Value);
            //}

            //Console.WriteLine($"SB1 -> filas SQL: {totalFilasSql}, códigos únicos: {productosDict.Count}");
            var jsonProductos = new List<string>();
            foreach (var kvp in productosDict)
            {
                Console.WriteLine("-- sb1 unico --");
                Console.WriteLine($"[SB1 JSON FINAL] codigo={kvp.Key}");
                Console.WriteLine(kvp.Value);
                jsonProductos.Add(kvp.Value);
            }

            Console.WriteLine($"SB1 -> filas SQL totales: {totalFilasSql}, códigos únicos: {productosDict.Count}");
            return jsonProductos;
        }
        public static List<string> jsonSB1_MBOM()
        {
            string connectionString = @"Data Source=DEPLM-11-PC\SQLEXPRESS;Initial Catalog=AgrometalBop;
                                Integrated Security=True;TrustServerCertificate=True";

            string query = @"
        WITH CTE_Hierarchy AS (
    SELECT DISTINCT
        Occurrence.id_table,
        ProductRevision.name,
        Product.productId AS codigo,
        CAST(Occurrence.parentRef AS INT) AS parentRef,
        ProductRevision.revision,
        ProductRevision.subType,
        Occurrence.idXml
    FROM Occurrence
    LEFT JOIN ProductRevision 
           ON Occurrence.instancedRef = ProductRevision.id_Table
          AND Occurrence.idXml       = ProductRevision.idXml
    LEFT JOIN Product 
           ON ProductRevision.masterRef = Product.id_Table
          AND ProductRevision.idXml    = Product.idXml
    GROUP BY
        Occurrence.id_table, ProductRevision.name, Product.productId,
        Occurrence.parentRef, ProductRevision.revision,
        ProductRevision.subType, Occurrence.idXml
)
SELECT DISTINCT
    COALESCE(Parent.name, '')              AS Nombre_Padre,
    COALESCE(CodFmt.CodigoPadre_Final, '') AS Codigo_Padre,      -- código padre formateado
    Child.name                             AS Nombre_Hijo,
    CodFmt.CodigoHijo_Final                AS Codigo_Hijo,       -- código hijo formateado
    Child.subType                          AS Subtype_Hijo,
    Qty.CantidadFinal                      AS CantidadHijo_Total,
    Child.revision                         AS Revision,

    /* ===== NUEVOS CAMPOS ALINEADOS CON SB1 ===== */
    MIN('PA')                              AS tipo,              -- mismo valor que SB1
    MIN('01')                              AS deposito,          -- mismo valor que SB1
    MAX(
        CASE 
            WHEN uudUnidad.title = 'Agm4_Unidad'     THEN uudUnidad.value
            WHEN uudUnidad.title = 'Agm4_Kilogramos' THEN uudUnidad.value
            WHEN uudUnidad.title = 'Agm4_Litros'     THEN uudUnidad.value
            WHEN uudUnidad.title = 'Agm4_Metros'     THEN uudUnidad.value
            ELSE 'UN'
        END
    )                                      AS unMedida
    /* =========================================== */

FROM CTE_Hierarchy Child
LEFT JOIN CTE_Hierarchy Parent 
       ON Child.parentRef = Parent.id_table
      -- AND Child.idXml   = Parent.idXml   -- según necesites

/* ==== JOIN PARA UNIDAD DE MEDIDA (copiado de SB1, adaptado a Child) ==== */
LEFT JOIN Form fUnidad
       ON Child.codigo = CASE
                            WHEN CHARINDEX('/', fUnidad.name) > 0 
                                THEN LEFT(fUnidad.name, CHARINDEX('/', fUnidad.name) - 1)
                            ELSE fUnidad.name
                         END
LEFT JOIN UserValue_UserData uudUnidad
       ON fUnidad.id_Table + 9 = uudUnidad.id_Father
      AND Child.idXml          = uudUnidad.idXml
/* ====================================================================== */

/* ---- CANTIDADES por ocurrencia de padre + código de hijo (como ya tenías) ---- */
LEFT JOIN (
    SELECT
        oPadre.id_Table AS ParentOccurrenceId,
        pHijo.productId AS ChildCodigo,
        CASE 
            WHEN prHijo.subType = 'Agm4_MatPrimaRevision'
                THEN SUM(TRY_CAST(uvud.value AS DECIMAL(18,6)))
            ELSE COUNT(DISTINCT oHijo.id_Table)
        END AS Cantidad
    FROM Product pHijo
    INNER JOIN ProductRevision prHijo 
            ON pHijo.id_Table = prHijo.masterRef
    LEFT JOIN Occurrence oHijo       
           ON oHijo.instancedRef = prHijo.id_Table
    LEFT JOIN UserValue_UserData uvud 
           ON uvud.id_Father = oHijo.id_Table + 2 
          AND uvud.title    = 'Quantity'
    LEFT JOIN Occurrence oPadre 
           ON oHijo.parentRef = oPadre.id_Table
    GROUP BY oPadre.id_Table, pHijo.productId, prHijo.subType
) sq3
    ON sq3.ParentOccurrenceId = Parent.id_table
   AND sq3.ChildCodigo        = Child.codigo

-- Cantidad final (1 si no hay padre; si hay, toma la calculada)
OUTER APPLY (
    SELECT CAST(
        CASE 
            WHEN Parent.id_table IS NULL THEN 1
            ELSE ISNULL(sq3.Cantidad, 1)
        END
    AS DECIMAL(18,2)) AS CantidadFinal
) AS Qty
OUTER APPLY (
    SELECT
        /* Padre: aplicar reglas SB1 si hay código */
        CASE 
            WHEN Parent.codigo IS NULL THEN NULL
            ELSE
                CASE 
                    WHEN LEFT(Parent.codigo, 2) = 'M-' THEN RIGHT(Parent.codigo, LEN(Parent.codigo) - 2)
                    WHEN LEFT(Parent.codigo, 1) = 'M'  THEN RIGHT(Parent.codigo, LEN(Parent.codigo) - 1)
                    WHEN LEFT(Parent.codigo, 1) = 'E'  THEN RIGHT(Parent.codigo, LEN(Parent.codigo) - 1)
                    WHEN RIGHT(Parent.codigo, 3) = '-FV' THEN LEFT(Parent.codigo, LEN(Parent.codigo) - 3)
                    ELSE Parent.codigo
                END
        END AS CodigoPadre_SB1,

        /* Hijo: aplicar mismas reglas */
        CASE 
            WHEN Child.codigo IS NULL THEN NULL
            ELSE
                CASE 
                    WHEN LEFT(Child.codigo, 2) = 'M-' THEN RIGHT(Child.codigo, LEN(Child.codigo) - 2)
                    WHEN LEFT(Child.codigo, 1) = 'M'  THEN RIGHT(Child.codigo, LEN(Child.codigo) - 1)
                    WHEN LEFT(Child.codigo, 1) = 'E'  THEN RIGHT(Child.codigo, LEN(Child.codigo) - 1)
                    WHEN RIGHT(Child.codigo, 3) = '-FV' THEN LEFT(Child.codigo, LEN(Child.codigo) - 3)
                    ELSE Child.codigo
                END
        END AS CodigoHijo_SB1
) CodSB1
/* ===================== FORMATEO DE CÓDIGOS (igual que antes) ===================== */
/* ====== DESPUÉS DE SB1: REGLA ADICIONAL DE 'E' Y TRATO ESPECIAL DEL HIJO RAÍZ ====== */
OUTER APPLY (
    SELECT
        -- Padre: si existe, trabajamos sobre CodigoPadre_SB1
        CASE 
            WHEN Parent.codigo IS NULL THEN NULL
            ELSE
                CASE 
                    WHEN LEFT(CodSB1.CodigoPadre_SB1, 1) = 'E'
                        THEN SUBSTRING(CodSB1.CodigoPadre_SB1, 2, LEN(CodSB1.CodigoPadre_SB1) - 1)
                    ELSE CodSB1.CodigoPadre_SB1
                END
        END AS CodigoPadre_SinE,

        -- Hijo:
        --   - Si NO tiene padre (raíz): tomamos directo el código ya normalizado por SB1
        --   - Si tiene padre: además aplicamos la regla de 'E' inicial como antes
        CASE 
            WHEN Parent.codigo IS NULL THEN CodSB1.CodigoHijo_SB1
            ELSE
                CASE 
                    WHEN LEFT(CodSB1.CodigoHijo_SB1, 1) = 'E'
                        THEN SUBSTRING(CodSB1.CodigoHijo_SB1, 2, LEN(CodSB1.CodigoHijo_SB1) - 1)
                    ELSE CodSB1.CodigoHijo_SB1
                END
        END AS CodigoHijo_SinE
) CodSinE
/* ====================================================================== */

/* ====================================================================== */


OUTER APPLY (
    SELECT
        CASE 
            WHEN CodSinE.CodigoPadre_SinE IS NULL THEN NULL
            ELSE
                CASE 
                    WHEN RIGHT(CodSinE.CodigoPadre_SinE, 2) LIKE '-[0-9]'
                        THEN LEFT(CodSinE.CodigoPadre_SinE, LEN(CodSinE.CodigoPadre_SinE) - 2)
                    ELSE CodSinE.CodigoPadre_SinE
                END
        END AS CodigoPadre_Final,

        CASE 
            WHEN Parent.codigo IS NULL THEN CodSinE.CodigoHijo_SinE
            ELSE
                CASE 
                    WHEN RIGHT(CodSinE.CodigoHijo_SinE, 2) LIKE '-[0-9]'
                        THEN LEFT(CodSinE.CodigoHijo_SinE, LEN(CodSinE.CodigoHijo_SinE) - 2)
                    ELSE CodSinE.CodigoHijo_SinE
                END
        END AS CodigoHijo_Final
) CodFmt
/* ============================================================================= */

WHERE
    Child.subType IN (
        'Agm4_ConGeneralRevision',
        'Agm4_MatPrimaRevision',
        'Agm4_PiezaRevision',
        'Agm4_RepCompradoRevision',
        'Agm4_SubConRevision',
        'Agm4_sub_mBOM_ERevision'
    )
GROUP BY
    Parent.name,
    CodFmt.CodigoPadre_Final,  -- códigos formateados
    Child.name,
    CodFmt.CodigoHijo_Final,
    Qty.CantidadFinal,
    Child.revision,
    Child.subType
ORDER BY
    Codigo_Padre;
    ";

            // 🔑 Diccionario para deduplicar por código
            var productosDict = new Dictionary<string, string>(); // codigo -> jsonData
            int totalFilasSql = 0;

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
                            //while (reader.Read())
                            //{
                            //    totalFilasSql++;

                            //    string codigo = reader["Codigo_Hijo"].ToString();
                            //    string descripcion = reader["Nombre_Hijo"].ToString();
                            //    string tipo = reader["Tipo"].ToString();
                            //    string deposito = reader["Deposito"].ToString();
                            //    string unMedida = reader["unMedida"].ToString();
                            //    string revision = reader["Revision"].ToString();
                            //    // string fantasma = reader["Fantasma"].ToString(); // si lo necesitás después

                            //    var producto = new
                            //    {
                            //        producto = new List<Dictionary<string, string>>
                            //{
                            //    new() { { "campo", "codigo"      }, { "valor", codigo      } },
                            //    new() { { "campo", "descripcion" }, { "valor", descripcion } },
                            //    new() { { "campo", "tipo"        }, { "valor", tipo        } },
                            //    new() { { "campo", "deposito"    }, { "valor", deposito    } },
                            //    new() { { "campo", "unMedida"    }, { "valor", unMedida    } },
                            //    new() { { "campo", "revEstruct"  }, { "valor", revision    } },
                            //    // new() { { "campo", "fantasma"    }, { "valor", fantasma    } },
                            //}
                            //    };

                            //    string jsonData = JsonConvert.SerializeObject(producto, Formatting.Indented);

                            //    // 👉 Opción: quedarte con el último JSON por código
                            //    productosDict[codigo] = jsonData;

                            //    // Si preferís quedarte con el primero y descartar el resto:
                            //    // if (!productosDict.ContainsKey(codigo))
                            //    //     productosDict[codigo] = jsonData;
                            //}
                            while (reader.Read())
                            {
                                totalFilasSql++;

                                string codigo = reader["Codigo_Hijo"].ToString();
                                string descripcion = reader["Nombre_Hijo"].ToString();
                                string tipo = reader["tipo"].ToString();
                                string deposito = reader["deposito"].ToString();
                                string unMedida = reader["unMedida"].ToString();
                                string revision = reader["Revision"].ToString();

                                Console.WriteLine(
                                    $"[SB1 SQL] codigo_hijo={codigo} | desc={descripcion} | tipo={tipo} | dep={deposito} | UM={unMedida} | rev={revision}"
                                );

                                var producto = new
                                {
                                    producto = new List<Dictionary<string, string>>
        {
            new() { { "campo", "codigo"      }, { "valor", codigo      } },
            new() { { "campo", "descripcion" }, { "valor", descripcion } },
            new() { { "campo", "tipo"        }, { "valor", tipo        } },
            new() { { "campo", "deposito"    }, { "valor", deposito    } },
            new() { { "campo", "unMedida"    }, { "valor", unMedida    } },
            new() { { "campo", "revEstruct"  }, { "valor", revision    } },
        }
                                };

                                string jsonData = JsonConvert.SerializeObject(producto, Formatting.Indented);

                                // log de deduplicación
                                if (productosDict.ContainsKey(codigo))
                                {
                                    Console.WriteLine($"[SB1] Código {codigo} ya existía, se reemplaza el JSON anterior por el nuevo.");
                                }
                                else
                                {
                                    Console.WriteLine($"[SB1] Nuevo código agregado al diccionario: {codigo}");
                                }

                                productosDict[codigo] = jsonData;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al consultar la base de datos: {ex.Message}");
            }

            //var jsonProductos = new List<string>();
            //foreach (var kvp in productosDict)
            //{
            //    Console.WriteLine("-- sb1 unico --");
            //    Console.WriteLine(kvp.Value);
            //    jsonProductos.Add(kvp.Value);
            //}

            //Console.WriteLine($"SB1 -> filas SQL: {totalFilasSql}, códigos únicos: {productosDict.Count}");
            var jsonProductos = new List<string>();
            foreach (var kvp in productosDict)
            {
                Console.WriteLine("-- sb1 unico --");
                Console.WriteLine($"[SB1 JSON FINAL] codigo={kvp.Key}");
                Console.WriteLine(kvp.Value);
                jsonProductos.Add(kvp.Value);
            }

            Console.WriteLine($"SB1 -> filas SQL totales: {totalFilasSql}, códigos únicos: {productosDict.Count}");
            return jsonProductos;
        }


        //public static List<string> jsonSB1()
        //{
        //    string connectionString = @"Data Source=DEPLM-11-PC\SQLEXPRESS;Initial Catalog=AgrometalBop;
        //                              Integrated Security=True;TrustServerCertificate=True";

        //    string query = @"SELECT DISTINCT
        //                 Product.id_Table,
        //                	 CASE WHEN LEFT(Product.productId, 2) = 'M-' THEN RIGHT(Product.productId, LEN(Product.productId) - 2)
        //                	WHEN LEFT(Product.productId, 1) = 'M' THEN RIGHT(Product.productId, LEN(Product.productId) - 1)
        //                 WHEN LEFT(Product.productId, 1) = 'E' THEN RIGHT(Product.productId, LEN(Product.productId)-1)
        //                	WHEN RIGHT(Product.productId, 3)= '-FV' THEN LEFT(Product.productId, LEN(Product.productId)-3)
        //                	 ELSE Product.productId END AS codigo,
        //                 LEFT(pr.name,60) as Descripcion,
        //                	'PA' as tipo,
        //                	'01' as deposito,
        //                	MAX(CASE
        //    	WHEN uud.title = 'Agm4_Unidad' THEN uud.value
        //    	WHEN uud.title = 'Agm4_Kilogramos' THEN uud.value
        //    	WHEN uud.title = 'Agm4_Litros' THEN uud.value
        //    	WHEN uud.title = 'Agm4_Metros' THEN uud.value
        //    	ELSE 'UN' END) AS 'unMedida',
        //    	pr.revision AS 'Revision',
        //    	CASE WHEN pr.name LIKE '%CONJ.CUBIERTAS%' OR pr.name LIKE '%GPS%' THEN
        //    	1 ELSE 0 END AS Fantasma
        //             FROM
        //                 Occurrence
        //             JOIN
        //                 ProductRevision pr ON Occurrence.instancedRef = pr.id_Table AND pr.idXml = Occurrence.idXml
        //             JOIN
        //                 Product ON pr.masterRef = Product.id_Table AND Product.idXml = pr.idXml
        //             LEFT JOIN
        //                 Form f ON Product.ProductId = (CASE
        //                     WHEN CHARINDEX('/', F.name) > 0 THEN LEFT(F.name, CHARINDEX('/', F.name) -1)
        //                     ELSE F.name END)
        //             JOIN UserValue_UserData uud ON
        //    f.id_Table + 9 = uud.id_Father
        //    AND Occurrence.idXml = uud.idXml
        //             --WHERE Product.productId = 'CDBZ50005A' OR Product.productId = 'CDBZ00004A' OR Product.productId = 'CDBZ00023A'
        //             --WHERE Product.productId = 'E086129'
        //             --WHERE Product.productId = '212019'
        //             GROUP BY
        //                 Product.id_Table,Product.productId, pr.name, uud.title, Occurrence.parentRef, pr.revision";


        //    List<string> jsonProductos = new List<string>();

        //    try
        //    {
        //        using (SqlConnection connection = new SqlConnection(connectionString))
        //        {
        //            connection.Open();

        //            using (SqlCommand command = new SqlCommand(query, connection))
        //            {
        //                command.CommandTimeout = 120;
        //                using (SqlDataReader reader = command.ExecuteReader())
        //                {

        //                    while (reader.Read())
        //                    {
        //                        string codigo = reader["codigo"].ToString();
        //                        string descripcion = reader["Descripcion"].ToString();
        //                        string tipo = reader["tipo"].ToString();
        //                        string deposito = reader["deposito"].ToString();
        //                        string unMedida = reader["unMedida"].ToString();
        //                        string revision = reader["Revision"].ToString();
        //                        //string fantasma = reader["Fantasma"].ToString();
        //                        // Construir el JSON para cada producto
        //                        var producto = new
        //                        {
        //                            producto = new List<Dictionary<string, string>>
        //                            {
        //                                new Dictionary<string, string> { { "campo", "codigo" }, { "valor",  codigo} },
        //                                new Dictionary<string, string> { { "campo", "descripcion" }, { "valor",  descripcion} },
        //                                new Dictionary<string, string> { { "campo", "tipo" }, { "valor",  tipo} },
        //                                new Dictionary<string, string> { { "campo", "deposito" }, { "valor",  deposito} },
        //                                new Dictionary<string, string> { { "campo", "unMedida" }, { "valor",  unMedida} },
        //                                new Dictionary<string, string> { { "campo", "revEstruct" },  {"valor", revision} },
        //                                //new Dictionary<string, string> { { "campo", "fantasma"}, {"valor",  fantasma} },
        //                            }
        //                        };

        //                        //poblarBase(codigo, descripcion, tipo, deposito, unMedida);
        //                        string jsonData = JsonConvert.SerializeObject(producto, Formatting.Indented);
        //                        Console.WriteLine(jsonData);
        //                        jsonProductos.Add(jsonData); // Guardar el JSON en la lista
        //                    }
        //                }
        //            }
        //        }
        //    }
        //    catch (Exception ex)
        //    {
        //        Console.WriteLine($"Error al consultar la base de datos: {ex.Message}");
        //    }

        //    return jsonProductos;
        //}

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
