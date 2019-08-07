using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.OleDb;
using System.IO;
using System.Data.SqlClient;
namespace DataMig
{
    class OrganizationProfiles
    {
        private string _sConnStr = @"Data Source=ERAM-GRA-003;" +
                                    "Initial Catalog=stampdev;" +
                                    "User id=sa;" +
                                    "Password=eram;";
        public void InsertOrganisation()
        {
            DataTable _dtOrgDet = ReadExcelFile("E:\\StampDB\\STAMP_IRIS.xlsx", "Organization Profiles");
            #region Data Proc And Ins
            int excelcount = 0;
            foreach (DataRow dr in _dtOrgDet.Rows)
            {
                //Console.WriteLine("Stamp_SMET_OragnisationProfiles==" + excelcount++);
                Int64 id = Int64.Parse(dr[0].ToString().Trim());
                InsOrgProfiles(
                        id,
                        5,
                        GetReferenceStHolderTypeId(dr[3].ToString().Trim()),
                        dr[2].ToString().Trim(),
                        dr[5].ToString().Trim(),
                        dr[4].ToString().Trim(),
                        "",
                        "",
                        "",
                        (dr[6].ToString().Trim() == "Yes") ? 1 : (dr[6].ToString().Trim() == "No") ? 2 : 3,
                        IsNumeric(dr[7].ToString().Trim()) ?  double.Parse(dr[7].ToString().Trim()) : 0,
                        "Active"
                    );
                
                if ("-" != dr[8].ToString().Trim() && "" != dr[8].ToString().Trim()) 
                {
                    InsOrganisationProfileEngRecord(
                        DateTime.Parse(dr[9].ToString().Trim()),
                        dr[8].ToString().Trim(),    
                        GetReferenceEngagementTypeId(dr[8].ToString().Trim()),
                        dr[10].ToString().Trim(),
                        dr[11].ToString().Trim(),
                        id
                        );
                }

                if ("-" != dr[12].ToString().Trim() && "" != dr[12].ToString().Trim())
                {
                    InsOrganisationProfileEngRecord(
                        DateTime.Parse(dr[13].ToString().Trim()),
                        dr[12].ToString().Trim(),
                        GetReferenceEngagementTypeId(dr[12].ToString().Trim()),
                        dr[14].ToString().Trim(),
                        dr[15].ToString().Trim(),
                        id
                        );
                }
                for (int i = 16; i <= 46; i += 3)
                {
                    
                    if ("-" != dr[i].ToString().Trim() && "" != dr[i].ToString().Trim())
                    {
                        InsOrgProfilesKeyInd(GetIndividualId(
                            dr[i].ToString().Trim()),
                            1, 1, id
                        );
                        InsOrganisationProfileRelevent(
                            dr[i].ToString().Trim(),
                            dr[i + 1].ToString().Trim(),
                            dr[i + 2].ToString().Trim(),
                            id
                       );
                    }
                }

                if ("" == dr[50].ToString().Trim())
                {
                    InsOrgProfilesSource(
                        dr[50].ToString().Trim(),
                        id
                        );
                }

            } 
            #endregion
        }

        public bool IsNumeric(string input)
        {
            int test;
            return int.TryParse(input, out test);
        }

        private void InsOrgProfiles(Int64 id, Int64 func_id, Int32 stht_id, string orgz_stakeholder_name, string orgz_description, string orgz_address, string orgz_callcenter, string orgz_email, string orgz_website, int orgz_bondholder, double orgz_totalholding, string staus_puplish)
        {
            string _query = "INSERT INTO [orgz_profiles] ([id], [func_id] ,[stht_id] ,[orgz_stakeholder_name] ,[orgz_description] ,[orgz_address] ,[orgz_callcenter] ,[orgz_email] ,[orgz_website] ,[orgz_bondholder] ,[orgz_totalholding] ,[staus_puplish]) VALUES (@id, @param1, @param2, @param3, @param4, @param5, @param6, @param7, @param8, @param9, @param10, @param11)";
            using (SqlConnection sqlConn = new SqlConnection(_sConnStr))
            {
                sqlConn.Open();
                using (SqlCommand cmd = new SqlCommand(_query, sqlConn))
                {
                    try
                    {
                        cmd.Parameters.Add("@id", SqlDbType.Int).Value = id;
                        cmd.Parameters.Add("@param1", SqlDbType.Int).Value = func_id;
                        cmd.Parameters.Add("@param2", SqlDbType.Int).Value = stht_id;
                        cmd.Parameters.Add("@param3", SqlDbType.VarChar, 255).Value = orgz_stakeholder_name;
                        cmd.Parameters.Add("@param4", SqlDbType.Text).Value = orgz_description;
                        cmd.Parameters.Add("@param5", SqlDbType.Text).Value = orgz_address;
                        cmd.Parameters.Add("@param6", SqlDbType.VarChar, 255).Value = orgz_callcenter;
                        cmd.Parameters.Add("@param7", SqlDbType.VarChar, 255).Value = orgz_email;
                        cmd.Parameters.Add("@param8", SqlDbType.VarChar, 255).Value = orgz_website;
                        cmd.Parameters.Add("@param9", SqlDbType.Int).Value = orgz_bondholder;
                        cmd.Parameters.Add("@param10", SqlDbType.Float).Value = orgz_totalholding;
                        cmd.Parameters.Add("@param11", SqlDbType.VarChar, 255).Value = staus_puplish;
                        cmd.CommandType = CommandType.Text;
                        cmd.ExecuteNonQuery();
                    }
                    catch (Exception Ex) { throw Ex; }
                }
            }
        }

        private DataTable ReadExcelFile(string _sFileName, string _sSheetName)
        {
            DataSet _dsDataSet = new DataSet();
            try
            {

                OleDbConnection con = new OleDbConnection("Provider=Microsoft.ACE.OLEDB.12.0;Data Source=" + _sFileName + ";Extended Properties=Excel 12.0");
                con.Open();
                OleDbDataAdapter myCommand = new OleDbDataAdapter(" SELECT * FROM [" + _sSheetName + "$]", con);
                myCommand.Fill(_dsDataSet);
                con.Close();
                DataTable _dtDataTable = _dsDataSet.Tables[0];
                return _dtDataTable;
            }
            catch (Exception ex)
            {
                //if you need to handle stuff
                Console.WriteLine(ex.Message);
            }
            finally
            {

            }
            return new DataTable();
        }

        private void InsOrgProfilesKeyInd(Int64 indv_id, Int32 dept_id, Int32 levl_id, Int64 orgz_id)
        {
            string _query = "INSERT INTO [orgz_profiles_keyinds] ([indv_id], [dept_id], [levl_id], [orgz_id]) VALUES (@param1, @param2, @param3, @param4)";
            using (SqlConnection sqlConn = new SqlConnection(_sConnStr))
            {
                sqlConn.Open();
                using (SqlCommand cmd = new SqlCommand(_query, sqlConn))
                {
                    try
                    {
                        cmd.Parameters.Add("@param1", SqlDbType.BigInt).Value = indv_id;
                        cmd.Parameters.Add("@param2", SqlDbType.Int).Value = dept_id;
                        cmd.Parameters.Add("@param3", SqlDbType.Int).Value = levl_id;
                        cmd.Parameters.Add("@param4", SqlDbType.BigInt).Value = orgz_id;
                        cmd.CommandType = CommandType.Text;
                        cmd.ExecuteNonQuery();
                    }
                    catch (Exception Ex)
                    { throw Ex; }
                }
            }
        }

        private void InsOrgProfilesSource(string srce_name, Int64 orgz_id)
        {
            string _query = "INSERT INTO orgz_profiles_source (srce_name, orgz_id) VALUES (@srce_name, @orgz_id)";
            using (SqlConnection sqlConn = new SqlConnection(_sConnStr))
            {
                sqlConn.Open();
                using (SqlCommand cmd = new SqlCommand(_query, sqlConn))
                {
                    try
                    {
                        cmd.Parameters.Add("@srce_name", SqlDbType.VarChar, 255).Value = srce_name;
                        cmd.Parameters.Add("@orgz_id", SqlDbType.BigInt).Value = orgz_id;
                        cmd.CommandType = CommandType.Text;
                        cmd.ExecuteNonQuery();
                    }
                    catch (Exception Ex)
                    { throw Ex; }
                }
            }
        }

        private void InsOrganisationProfileRelevent(string _sIndName, string _sPosIdOne, string _sPosIdTwo, Int64 OrgId)
        {
            string _query = "INSERT INTO [orgz_profiles_relevant] ([relv_individual] ,[post_id_1] ,[post_id_2], [orgz_id]) VALUES (@param1, @param2, @param3, @param4)";
            using (SqlConnection sqlConn = new SqlConnection(_sConnStr))
            {
                sqlConn.Open();
                using (SqlCommand cmd = new SqlCommand(_query, sqlConn))
                {
                    try
                    {
                        cmd.Parameters.Add("@param1", SqlDbType.VarChar, 255).Value = _sIndName;
                        cmd.Parameters.Add("@param2", SqlDbType.Int).Value = GetReferencePositionId(_sPosIdOne);
                        cmd.Parameters.Add("@param3", SqlDbType.Int).Value = GetReferencePositionId(_sPosIdTwo);
                        cmd.Parameters.Add("@param4", SqlDbType.BigInt).Value = OrgId;
                        cmd.CommandType = CommandType.Text;
                        cmd.ExecuteReader();
                    }
                    catch (Exception Ex)
                    { throw Ex; }
                }
            }
        }

        private int InsOrganisationProfileEngRecord(DateTime engr_date, string engr_topic, Int32 engt_id, string engr_picpertamina, string engr_picinvestor, Int64 orgz_id)
        {
            string _query = "INSERT INTO [orgz_profiles_engrecord] ([engr_date] , [engr_topic], [engt_id], [engr_picpertamina], [engr_picinvestor],[orgz_id]) VALUES (@engr_date ,@engr_topic ,@engt_id ,@engr_picpertamina,@engr_picinvestor, @orgz_id)";
            using (SqlConnection sqlConn = new SqlConnection(_sConnStr))
            {
                sqlConn.Open();
                using (SqlCommand cmd = new SqlCommand(_query, sqlConn))
                {
                    try{
                        cmd.Parameters.Add("@engr_date", SqlDbType.DateTime).Value = engr_date;
                        cmd.Parameters.Add("@engr_topic", SqlDbType.VarChar, 255).Value = engr_topic;
                        cmd.Parameters.Add("@engt_id", SqlDbType.Int).Value = engt_id;
                        cmd.Parameters.Add("@engr_picpertamina", SqlDbType.VarChar, 255).Value = engr_picpertamina;
                        cmd.Parameters.Add("@engr_picinvestor", SqlDbType.VarChar, 255).Value = engr_picinvestor;
                        cmd.Parameters.Add("@orgz_id", SqlDbType.BigInt).Value = orgz_id;
                        cmd.CommandType = CommandType.Text;
                        return (int)cmd.ExecuteNonQuery();
                    }
                    catch (Exception Ex)
                    { throw Ex; }
                }
            }
        }

        private int GetReferencePositionId(string _sReferencePositionName)
        {
            string query = "SELECT [id] FROM [reff_position] WHERE [value] = @param1";

            using (SqlConnection sqlConn = new SqlConnection(_sConnStr))
            {
                sqlConn.Open();
                using (SqlCommand cmd = new SqlCommand(query, sqlConn))
                {
                    try
                    {
                        cmd.Parameters.Add("@param1", SqlDbType.VarChar, 255).Value = _sReferencePositionName;
                        DataTable dt = new DataTable();
                        dt.Load(cmd.ExecuteReader());
                        return (dt.Rows.Count > 0) ? int.Parse(dt.Rows[0][0].ToString().Trim()) : 0;
                    }
                    catch (Exception Ex)
                    { throw Ex; }
                }
            }
        }

        private int GetReferenceStHolderTypeId(string _sReferenceStHolderName)
        {
            string query = "SELECT [id] FROM [reff_stakeholdertype] WHERE [value] = @param1";

            using (SqlConnection sqlConn = new SqlConnection(_sConnStr))
            {
                sqlConn.Open();
                using (SqlCommand cmd = new SqlCommand(query, sqlConn))
                {
                    try
                    {
                        cmd.Parameters.Add("@param1", SqlDbType.VarChar, 255).Value = _sReferenceStHolderName;
                        DataTable dt = new DataTable();
                        dt.Load(cmd.ExecuteReader());
                        return (dt.Rows.Count > 0) ? int.Parse(dt.Rows[0][0].ToString().Trim()) : 0;
                    }
                    catch (Exception Ex)
                    { throw Ex; }
                }
            }
        }

        private int GetReferenceEngagementTypeId(string _sEngagementTypeName)
        {
            string query = "SELECT [id] FROM [reff_engagementtype] WHERE [value] = @param1";

            using (SqlConnection sqlConn = new SqlConnection(_sConnStr))
            {
                sqlConn.Open();
                using (SqlCommand cmd = new SqlCommand(query, sqlConn))
                {
                    try
                    {
                        cmd.Parameters.Add("@param1", SqlDbType.VarChar, 255).Value = _sEngagementTypeName;
                        DataTable dt = new DataTable();
                        dt.Load(cmd.ExecuteReader());
                        return (dt.Rows.Count > 0) ? int.Parse(dt.Rows[0][0].ToString().Trim()) : 0;
                    }
                    catch (Exception Ex)
                    { throw Ex; }
                }
            }
        }

        private int GetIndividualId(string _indv_name)
        {
            string query = "SELECT [id] FROM [indv_profiles] WHERE [indv_name] = @param1";

            using (SqlConnection sqlConn = new SqlConnection(_sConnStr))
            {
                sqlConn.Open();
                using (SqlCommand cmd = new SqlCommand(query, sqlConn))
                {
                    try
                    {
                        cmd.Parameters.Add("@param1", SqlDbType.VarChar, 255).Value = _indv_name;
                        DataTable dt = new DataTable();
                        dt.Load(cmd.ExecuteReader());
                        return (dt.Rows.Count > 0) ? int.Parse(dt.Rows[0][0].ToString().Trim()) : 0;
                    }
                    catch (Exception Ex)
                    { throw Ex; }
                }
            }
        }
    }
}
