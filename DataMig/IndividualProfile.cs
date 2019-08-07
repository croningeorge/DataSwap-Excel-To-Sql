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
    class IndividualProfile
    {
        private string _sConnStr = @"Data Source=ERAM-GRA-003;" +
                                   "Initial Catalog=stampdev;" +
                                   "User id=sa;" +
                                   "Password=eram;";
        public void InsertIndividual()
        {

            DataTable _dtOrgDet = ReadExcelFile("E:\\StampDB\\STAMP_IRIS.xlsx", "InvProfile_Invester");
            #region Data Proc And Ins
            int excelcount = 0;

            foreach (DataRow dr in _dtOrgDet.Rows)
            {
                //Console.WriteLine("InvProfile_Invester==" + excelcount++);
                // Console.ReadKey();
                Int64 id = Int64.Parse(dr[0].ToString().Trim());
                InsOrgProfiles(
                    id,
                    5,
                    dr[4].ToString().Trim(),
                    dr[5].ToString().Trim(),
                    dr[6].ToString().Trim(),
                    (dr[3].ToString().Trim() == "Yes") ? 1 : (dr[3].ToString().Trim() == "No") ? 2 : 3,
                    dr[1].ToString().Trim(),
                    dr[12].ToString().Trim(),
                    dr[13].ToString().Trim(),
                    dr[14].ToString().Trim(),
                    dr[15].ToString().Trim(),
                    dr[16].ToString().Trim(),
                    DateTime.Parse(dr[34].ToString().Trim()),
                    GetReferenceStHolderTypeId(dr[2].ToString().Trim()),
                    AssetReferenceType(dr[7].ToString().Trim()),
                    LocationType(dr[8].ToString().Trim())
                    );

                if ("-" != dr[28].ToString().Trim() && "" != dr[28].ToString().Trim())
                {
                    InsOrganisationProfileEngRecord(
                        DateTime.Parse(dr[28].ToString().Trim()),
                        dr[28].ToString().Trim(),
                        GetReferenceEngagementTypeId(dr[29].ToString().Trim()),
                        id
                        );
                }
                if ("" == dr[34].ToString().Trim())
                {
                    InsOrgProfilesSource(
                        dr[34].ToString().Trim(),
                        id
                        );
                }

                if ("-" != dr[9].ToString().Trim() && "" != dr[9].ToString().Trim())
                {
                    InsOrganisationPosition(
                        dr[9].ToString().Trim(),
                        GetReferencePositionId(dr[10].ToString().Trim()
                        ));


                }
                for (int i = 17; i <= 24; i++)
                {
                    if ("-" != dr[i].ToString().Trim() && "" != dr[i].ToString().Trim())
                    {
                        InsertCareer(
                            dr[i].ToString().Trim(),
                            id);
                    }
                }
                for (int i = 25; i <= 27; i++)
                {
                    if ("-" != dr[i].ToString().Trim() && "" != dr[i].ToString().Trim())
                    {
                        InsertEducation(
                           dr[i].ToString().Trim(),
                           id);
                    }
                }
                for (int i = 30; i <= 33; i++)
                {
                    if ("-" != dr[i].ToString().Trim() && "" != dr[i].ToString().Trim())
                    {
                        InsertSource(
                            dr[i].ToString().Trim(),
                            id);
                    }
                }

                if ("-" != dr[7].ToString().Trim() && "" != dr[7].ToString().Trim())
                {
                    InsertAssetClass(
                        dr[7].ToString().Trim(),
                        GetReferencePositionId(dr[8].ToString().Trim()
                        ));
                }


            }
            #endregion
        }

        private void InsertAssetClass(string value, Int64 indv_id)
        {
            string _query = "INSERT INTO [reff_assetclass]([value]) VALUES (@value)";
            using (SqlConnection sqlConn = new SqlConnection(_sConnStr))
            {
                sqlConn.Open();
                using (SqlCommand cmd = new SqlCommand(_query, sqlConn))
                {
                    try
                    {
                        cmd.Parameters.Add("@value", SqlDbType.VarChar, 255).Value = value;
                        //cmd.Parameters.Add("@indv_id", SqlDbType.BigInt).Value = indv_id;
                        cmd.CommandType = CommandType.Text;
                        cmd.ExecuteNonQuery();
                    }
                    catch (Exception Ex)
                    { throw Ex; }
                }

            }
        }

        private int GetReferenceAssetclass(string _sassetname)
        {
            string query = "SELECT [id] FROM [reff_assetclass] WHERE [value] = @param1";

            using (SqlConnection sqlConn = new SqlConnection(_sConnStr))
            {
                sqlConn.Open();
                using (SqlCommand cmd = new SqlCommand(query, sqlConn))
                {
                    try
                    {
                        cmd.Parameters.Add("@param1", SqlDbType.VarChar, 255).Value = _sassetname;
                        DataTable dt = new DataTable();
                        dt.Load(cmd.ExecuteReader());
                        return (dt.Rows.Count > 0) ? int.Parse(dt.Rows[0][0].ToString().Trim()) : 0;
                    }
                    catch (Exception Ex)
                    { throw Ex; }
                }
            }
        }

        private void InsertSource(string srce_link, Int64 indv_id)
        {
            string _query = "INSERT INTO [indv_profiles_source]([srce_link],[indv_id]) VALUES (@srce_link,@indv_id)";
            using (SqlConnection sqlConn = new SqlConnection(_sConnStr))
            {
                sqlConn.Open();
                using (SqlCommand cmd = new SqlCommand(_query, sqlConn))
                {
                    try
                    {
                        cmd.Parameters.Add("@srce_link", SqlDbType.VarChar, 255).Value = srce_link;
                        cmd.Parameters.Add("@indv_id", SqlDbType.BigInt).Value = indv_id;
                        cmd.CommandType = CommandType.Text;
                        cmd.ExecuteNonQuery();
                    }
                    catch (Exception Ex)
                    { throw Ex; }
                }

            }
        }

        private void InsertCareer(string careername, Int64 indv_id)
        {
            string _query = "INSERT INTO [indv_profiles_career]([carr_name],[indv_id]) VALUES (@carr_name,@indv_id)";
            using (SqlConnection sqlConn = new SqlConnection(_sConnStr))
            {
                sqlConn.Open();
                using (SqlCommand cmd = new SqlCommand(_query, sqlConn))
                {
                    try
                    {
                        cmd.Parameters.Add("@carr_name", SqlDbType.VarChar, 255).Value = careername;
                        cmd.Parameters.Add("@indv_id", SqlDbType.BigInt).Value = indv_id;
                        cmd.CommandType = CommandType.Text;
                        cmd.ExecuteNonQuery();
                    }
                    catch (Exception Ex)
                    { throw Ex; }
                }

            }
        }

        private void InsertEducation(string educ_name, Int64 indv_id)
        {
            string _query = "INSERT INTO [indv_profiles_education]([educ_name],[indv_id]) VALUES (@educ_name,@indv_id)";
            using (SqlConnection sqlConn = new SqlConnection(_sConnStr))
            {
                sqlConn.Open();
                using (SqlCommand cmd = new SqlCommand(_query, sqlConn))
                {
                    try
                    {
                        cmd.Parameters.Add("@educ_name", SqlDbType.VarChar, 255).Value = educ_name;
                        cmd.Parameters.Add("@indv_id", SqlDbType.BigInt).Value = indv_id;
                        cmd.CommandType = CommandType.Text;
                        cmd.ExecuteNonQuery();
                    }
                    catch (Exception Ex)
                    { throw Ex; }
                }

            }
        }

        public bool IsNumeric(string input)
        {
            int test;
            return int.TryParse(input, out test);
        }

        private void InsOrgProfiles(Int64 id, Int64 func_id, string indv_stakeholder_name, string indv_stakeholder_name_others, string indv_stakeholder_group, int indv_bondholder, string indv_name, string indv_address, string indv_cell_number, string indv_office_number, string indv_email1, string indv_email2, DateTime staus_puplish, int stht_id, int ascs_id, int loct_id)
        {
            string _query = "INSERT INTO [indv_profiles] ([id],[func_id] ,[indv_stakeholder_name] ,[indv_stakeholder_name_others] ,[indv_stakeholder_group],[indv_bondholder] ,[indv_name],[indv_address] ,[indv_cell_number],[indv_office_number],[indv_email1],[indv_email2],[staus_puplish],[stht_id],[ascs_id],[loct_id]) VALUES (@id, @param1, @param2, @param3, @param4, @param5, @param6, @param7, @param8, @param9, @param10,@param11,@param12,@param13,@param14,@param15)";
            using (SqlConnection sqlConn = new SqlConnection(_sConnStr))
            {
                sqlConn.Open();
                using (SqlCommand cmd = new SqlCommand(_query, sqlConn))
                {
                    try
                    {
                        cmd.Parameters.Add("@id", SqlDbType.Int).Value = id;
                        cmd.Parameters.Add("@param1", SqlDbType.Int).Value = func_id;
                        cmd.Parameters.Add("@param2", SqlDbType.VarChar, 255).Value = indv_stakeholder_name;
                        cmd.Parameters.Add("@param3", SqlDbType.VarChar, 255).Value = indv_stakeholder_name_others;
                        cmd.Parameters.Add("@param4", SqlDbType.VarChar, 255).Value = indv_stakeholder_group;
                        cmd.Parameters.Add("@param5", SqlDbType.Int).Value = indv_bondholder;

                        cmd.Parameters.Add("@param6", SqlDbType.VarChar, 255).Value = indv_name;
                        //cmd.Parameters.Add("@param7", SqlDbType.Date).Value = indv_birthdate;
                        cmd.Parameters.Add("@param7", SqlDbType.VarChar, 255).Value = indv_address;
                        cmd.Parameters.Add("@param8", SqlDbType.VarChar, 255).Value = indv_cell_number;
                        cmd.Parameters.Add("@param9", SqlDbType.VarChar, 255).Value = indv_office_number;
                        cmd.Parameters.Add("@param10", SqlDbType.VarChar, 255).Value = indv_email1;
                        cmd.Parameters.Add("@param11", SqlDbType.VarChar, 255).Value = indv_email2;
                        cmd.Parameters.Add("@param12", SqlDbType.Date).Value = staus_puplish;
                        cmd.Parameters.Add("@param13", SqlDbType.Int).Value = stht_id;
                        cmd.Parameters.Add("@param14", SqlDbType.Int).Value = ascs_id;
                        cmd.Parameters.Add("@param15", SqlDbType.Int).Value = loct_id;
                        cmd.CommandType = CommandType.Text;

                        object y = cmd.ExecuteScalar();


                    }
                    catch (Exception Ex)
                    {
                        throw Ex;
                    }
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
            string _query = "INSERT INTO [orgz_profiles_keyinds] ([indv_id], [dept_id], [levl_id], [orgz_id]) VALUES (param1, param2, param3, param4)";
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

        private void InsOrgProfilesSource(string srce_link, Int64 indv_id)
        {
            string _query = "INSERT INTO [indv_profiles_source] ([srce_link],[indv_id]) VALUES (@srce_link,@indv_id)";
            using (SqlConnection sqlConn = new SqlConnection(_sConnStr))
            {
                sqlConn.Open();
                using (SqlCommand cmd = new SqlCommand(_query, sqlConn))
                {
                    try
                    {
                        cmd.Parameters.Add("@srce_link", SqlDbType.VarChar, 255).Value = srce_link;
                        cmd.Parameters.Add("@indv_id", SqlDbType.BigInt).Value = indv_id;
                        cmd.CommandType = CommandType.Text;
                        cmd.ExecuteNonQuery();
                    }
                    catch (Exception Ex)
                    { throw Ex; }
                }
            }
        }

        private void InsOrganisationPosition(string pstn_name, Int64 indv_id)
        {
            string _query = "INSERT INTO [indv_profiles_position] ([pstn_name] ,[indv_id]) VALUES (@param1, @param2)";
            using (SqlConnection sqlConn = new SqlConnection(_sConnStr))
            {
                sqlConn.Open();
                using (SqlCommand cmd = new SqlCommand(_query, sqlConn))
                {
                    try
                    {
                        cmd.Parameters.Add("@param1", SqlDbType.VarChar, 255).Value = pstn_name;
                        cmd.Parameters.Add("@param2", SqlDbType.BigInt).Value = indv_id;
                        cmd.CommandType = CommandType.Text;
                        cmd.ExecuteReader();
                    }
                    catch (Exception Ex)
                    { throw Ex; }
                }
            }
        }

        private void InsOrganisationEducation(string pstn_name, Int64 indv_id)
        {
            string _query = "INSERT INTO [indv_profiles_position] ([pstn_name] ,[indv_id]) VALUES (@param1, @param2)";
            using (SqlConnection sqlConn = new SqlConnection(_sConnStr))
            {
                sqlConn.Open();
                using (SqlCommand cmd = new SqlCommand(_query, sqlConn))
                {
                    try
                    {
                        cmd.Parameters.Add("@param1", SqlDbType.VarChar, 255).Value = pstn_name;
                        cmd.Parameters.Add("@param2", SqlDbType.BigInt).Value = indv_id;
                        cmd.CommandType = CommandType.Text;
                        cmd.ExecuteReader();
                    }
                    catch (Exception Ex)
                    { throw Ex; }
                }
            }
        }

        private int InsOrganisationProfileEngRecord(DateTime engr_date, string engr_topic, Int32 engt_id, Int64 indv_id)
        {
            string _query = "INSERT INTO [indv_profiles_engrecord] ([engr_date] , [engr_topic], [engt_id],[indv_id]) VALUES (@engr_date ,@engr_topic ,@engt_id , @indv_id)";
            using (SqlConnection sqlConn = new SqlConnection(_sConnStr))
            {
                sqlConn.Open();
                using (SqlCommand cmd = new SqlCommand(_query, sqlConn))
                {
                    try
                    {
                        cmd.Parameters.Add("@engr_date", SqlDbType.DateTime).Value = engr_date;
                        cmd.Parameters.Add("@engr_topic", SqlDbType.VarChar, 255).Value = engr_topic;
                        cmd.Parameters.Add("@engt_id", SqlDbType.Int).Value = engt_id;
                        //cmd.Parameters.Add("@engr_picpertamina", SqlDbType.VarChar, 255).Value = engr_picpertamina;
                        //cmd.Parameters.Add("@engr_picinvestor", SqlDbType.VarChar, 255).Value = engr_picinvestor;
                        cmd.Parameters.Add("@indv_id", SqlDbType.BigInt).Value = indv_id;
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

        private int AssetReferenceType(string _sRefassetName)
        {
            string query = "SELECT [id] FROM [reff_assetclass] WHERE [value] = @param1";

            using (SqlConnection sqlConn = new SqlConnection(_sConnStr))
            {
                sqlConn.Open();
                using (SqlCommand cmd = new SqlCommand(query, sqlConn))
                {
                    try
                    {
                        cmd.Parameters.Add("@param1", SqlDbType.VarChar, 255).Value = _sRefassetName;
                        DataTable dt = new DataTable();
                        dt.Load(cmd.ExecuteReader());
                        return (dt.Rows.Count > 0) ? int.Parse(dt.Rows[0][0].ToString().Trim()) : 0;
                    }
                    catch (Exception Ex)
                    { throw Ex; }
                }
            }
        }

        private int LocationType(string _slocationName)
        {
            string query = "SELECT [id] FROM [reff_location] WHERE [value] = @param1";

            using (SqlConnection sqlConn = new SqlConnection(_sConnStr))
            {
                sqlConn.Open();
                using (SqlCommand cmd = new SqlCommand(query, sqlConn))
                {
                    try
                    {
                        cmd.Parameters.Add("@param1", SqlDbType.VarChar, 255).Value = _slocationName;
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

        private bool IsDate(string _sDate)
        {
            try
            {
                DateTime dt = DateTime.Parse(_sDate);
                return true;
            }
            catch
            { return false; }
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

        private int GetReferenceStakeholderType(string _sStakeholderType)
        {
            string query = "SELECT [id] FROM [reff_stakeholdertype] WHERE [value] = @param1";

            using (SqlConnection sqlConn = new SqlConnection(_sConnStr))
            {
                sqlConn.Open();
                using (SqlCommand cmd = new SqlCommand(query, sqlConn))
                {
                    try
                    {
                        cmd.Parameters.Add("@param1", SqlDbType.VarChar, 255).Value = _sStakeholderType;
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
