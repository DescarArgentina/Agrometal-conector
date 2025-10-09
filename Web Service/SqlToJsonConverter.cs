namespace Web_Service
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.SqlClient;
    using System.Linq;
    using System.Text.RegularExpressions;
    using Newtonsoft.Json;
    using System.Globalization;

    public class ProductStructure
    {
        public string producto { get; set; }
        public string qtdBase { get; set; }
        public List<List<Campo>> estructura { get; set; } = new List<List<Campo>>();
    }

    public class Campo
    {
        public string campo { get; set; }
        public string valor { get; set; }
    }

    public class SqlRecord
    {
        public string ProcessName { get; set; }
        public string Codigo { get; set; }
        public double Cantidad { get; set; }
        public string Subtype { get; set; }  // <-- nuevo campo

        public string Nombre_WA { get; set; }
    }

    public class SqlToJsonConverter
    {
        private readonly string connectionString;

        public SqlToJsonConverter(string connectionString)
        {
            this.connectionString = connectionString;
        }

        public List<ProductStructure> ConvertSqlToHierarchicalJsons(string sqlQuery)
        {
            var records = new List<SqlRecord>();

            using (var conn = new SqlConnection(connectionString))
            {
                conn.Open();
                using (var cmd = new SqlCommand(sqlQuery, conn))
                using (var rdr = cmd.ExecuteReader())
                {
                    while (rdr.Read())
                    {
                        records.Add(new SqlRecord
                        {
                            ProcessName = rdr["Process_name"].ToString(),
                            Codigo = rdr["codigo"].ToString(),
                            Cantidad = Convert.ToDouble(rdr["Cantidad"]),
                            Subtype = rdr["subType"].ToString(),
                            Nombre_WA = rdr["Nombre_WA"].ToString()
                        });
                    }
                }
            }

            if (!records.Any())
                return new List<ProductStructure>();

            var match = Regex.Match(records[0].ProcessName, @"\d+");
            string rootParent = match.Success ? match.Value : records[0].ProcessName;

            // >>> Agrupación por padre
            var map = new Dictionary<string, ProductStructure>();
            var parentOrder = new List<string>(); // para preservar el orden
            string lastMEProcessRevision = null;

            for (int i = 0; i < records.Count; i++)
            {
                var rec = records[i];
                bool isTerceros = !string.IsNullOrWhiteSpace(rec.Nombre_WA) &&
                                  rec.Nombre_WA.IndexOf("terceros", StringComparison.OrdinalIgnoreCase) >= 0;

                string padre;

                if (isTerceros && i + 1 < records.Count)
                {
                    // Hacer hermano del de abajo: usar el MISMO padre que tendrá el próximo registro
                    var nextRec = records[i + 1];

                    // El padre del próximo se calcula con el lastMEProcessRevision ACTUAL (aún sin procesar nextRec)
                    string padreNext = (nextRec.Subtype == "MEProcessRevision")
                        ? (lastMEProcessRevision ?? rootParent)
                        : (lastMEProcessRevision ?? rootParent);

                    padre = padreNext;

                    // Nota: NO actualizamos lastMEProcessRevision aquí aunque rec sea MEProcessRevision.
                    // La idea es que este registro no afecte la cadena; solo se cuelga como hermano del siguiente.
                }
                else
                {
                    if (rec.Subtype == "MEProcessRevision")
                    {
                        // Su padre es el último MEProcessRevision anterior (o root si no hay)
                        padre = lastMEProcessRevision ?? rootParent;

                        // Ahora sí, al ser un MEProcessRevision real, actualiza el tracker
                        lastMEProcessRevision = rec.Codigo;
                    }
                    else
                    {
                        // No es MEProcessRevision => cuelga del último MEProcessRevision (o root si es el primero)
                        padre = lastMEProcessRevision ?? rootParent;
                    }
                }

                if (!map.TryGetValue(padre, out var product))
                {
                    product = new ProductStructure
                    {
                        producto = padre,
                        qtdBase = "1",
                        estructura = new List<List<Campo>>()
                    };
                    map[padre] = product;
                    parentOrder.Add(padre);
                }

                product.estructura.Add(new List<Campo>
    {
        new Campo { campo = "codigo",   valor = rec.Codigo },
        new Campo { campo = "cantidad", valor = rec.Cantidad.ToString(CultureInfo.InvariantCulture) }
    });
            }

            // Devolver respetando el orden de aparición de los padres
            var products = new List<ProductStructure>(parentOrder.Count);
            foreach (var padre in parentOrder)
                products.Add(map[padre]);

            return products;

        }




        public List<string> ConvertToHierarchicalJsonStrings(string sqlQuery)
        {
            var products = ConvertSqlToHierarchicalJsons(sqlQuery);
            return products
                .Select(p => JsonConvert.SerializeObject(p, Formatting.Indented))
                .ToList();
        }

        // Método para mostrar la estructura jerárquica
        public void ShowHierarchicalStructure(string sqlQuery)
        {
            var products = ConvertSqlToHierarchicalJsons(sqlQuery);

            Console.WriteLine("ESTRUCTURA JERÁRQUICA:");
            Console.WriteLine(new string('=', 50));

            for (int i = 0; i < products.Count; i++)
            {
                var product = products[i];
                var hijo = product.estructura[0].First(c => c.campo == "codigo").valor;

                Console.WriteLine($"Relación #{i + 1}:");
                Console.WriteLine($"  Padre: {product.producto}");
                Console.WriteLine($"  Hijo:  {hijo}");

                if (i == 0)
                {
                    Console.WriteLine($"  Nota: Primera línea - Padre es instancedWorkArea");
                }
                else
                {
                    Console.WriteLine($"  Nota: Línea {i + 1} - Padre es el código de la línea {i}");
                }

                Console.WriteLine();
            }
        }

        // Método para guardar cada JSON en archivos separados con nombres descriptivos
        public void SaveHierarchicalJsonFiles(string sqlQuery, string basePath = "")
        {
            var products = ConvertSqlToHierarchicalJsons(sqlQuery);

            for (int i = 0; i < products.Count; i++)
            {
                var product = products[i];
                var hijo = product.estructura[0].First(c => c.campo == "codigo").valor;

                string json = JsonConvert.SerializeObject(product, Formatting.Indented);
                string fileName = $"{basePath}relacion_{i + 1:D3}_padre_{product.producto}_hijo_{hijo}.json";

                System.IO.File.WriteAllText(fileName, json);
                Console.WriteLine($"Guardado: {fileName}");
            }
        }

        // Método para procesar y mostrar cada JSON individual
        public void ProcessHierarchicalJsons(string sqlQuery)
        {
            var products = ConvertSqlToHierarchicalJsons(sqlQuery);

            Console.WriteLine($"Total de JSONs jerárquicos generados: {products.Count}");
            Console.WriteLine(new string('=', 50));

            for (int i = 0; i < products.Count; i++)
            {
                var product = products[i];
                string json = JsonConvert.SerializeObject(product, Formatting.Indented);

                Console.WriteLine($"JSON #{i + 1}:");
                Console.WriteLine(json);
                Console.WriteLine(new string('-', 30));
            }
        }

        // Método para generar un resumen de la cadena jerárquica
        public void ShowHierarchicalChain(string sqlQuery)
        {
            var products = ConvertSqlToHierarchicalJsons(sqlQuery);

            Console.WriteLine("CADENA JERÁRQUICA COMPLETA:");
            Console.WriteLine(new string('=', 50));

            if (products.Count > 0)
            {
                // Mostrar el primer padre (instancedWorkArea)
                Console.WriteLine($"Raíz: {products[0].producto}");

                // Mostrar la cadena
                for (int i = 0; i < products.Count; i++)
                {
                    var hijo = products[i].estructura[0].First(c => c.campo == "codigo").valor;
                    Console.WriteLine($"  +- Nivel {i + 1}: {hijo}");
                }
            }
        }
    }
}


