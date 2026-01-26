using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Web_Service
{
    
    public class OperacionSQL
    {
        public string WorkareaName { get; set; }
        public int Operacion { get; set; }
        public string Codigo { get; set; }
        public string Descripcion { get; set; }
        public string Tipo { get; set; }
        public string Deposito { get; set; }
        public string UnidadMedida { get; set; }
        public int IdXml { get; set; }
    }

    // Modelo para la jerarquía
    public class OperacionJerarquica
    {
        public string WorkareaName { get; set; }
        public int NumeroOperacion { get; set; }
        public string Codigo { get; set; }
        public string Descripcion { get; set; }
        public string Tipo { get; set; }
        public string Deposito { get; set; }
        public string UnidadMedida { get; set; }
        public int IdXml { get; set; }

        public OperacionJerarquica Padre { get; set; }
        public List<OperacionJerarquica> Hijos { get; set; } = new List<OperacionJerarquica>();
        public int Nivel { get; set; }
    }

    public class CampoValor
    {
        [JsonProperty("campo")]
        public string Campo { get; set; }

        [JsonProperty("valor")]
        public string Valor { get; set; }
    }

    public class EstructuraProducto
    {
        [JsonProperty("producto")]
        public string Producto { get; set; }

        [JsonProperty("qtdBase")]
        public string QtdBase { get; set; }

        [JsonProperty("estructura")]
        public List<List<CampoValor>> Estructura { get; set; } = new List<List<CampoValor>>();
    }

}
