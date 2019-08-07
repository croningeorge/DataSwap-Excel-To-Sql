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
    class SMETIndividual
    {
        private string _sConnStr = @"Data Source=ERAM-GRA-003;" +
                                   "Initial Catalog=stampdev;" +
                                   "User id=sa;" +
                                   "Password=eram;";
        public void InsertSmetIndividual()
        {
            DataTable _dtOrgDet = ReadExcelFile("C:\\StampDB\\STAMP_SMET.xlsx", "Individual Profiles");
            #region Data Proc And Ins
            int excelcount = 0;
            foreach (DataRow dr in _dtOrgDet.Rows)
            {

                //Console.WriteLine("Stamp_SMET_IndividualProfile=="+excelcount++);

                Int64 id = Int64.Parse(dr[0].ToString().Trim());
                InsInvProfiles(
                    id,
                    4,
                    dr[3].ToString().Trim(),//organisation
                    dr[4].ToString().Trim(),//name
                    dr[5].ToString().Trim(),//title
                    dr[8].ToString().Trim(),
                    //isdatenullable,//birthdate
                    dr[7].ToString().Trim(),//address
                    dr[8].ToString().Trim(),//phonenumber
                    dr[9].ToString().Trim(),//email
                    dr[10].ToString().Trim(),//spousename
                    dr[21].ToString().Trim(),//personal intreast
                    dr[52].ToString().Trim(),//personal Reference
                    dr[53].ToString().Trim(),//stakeholder Responsibility
                    dr[88].ToString().Trim(),//Remarks
                    dr[92].ToString().Trim()//updatedate
                    );

                //children 
                for (int i = 11; i <= 20; i++)
                {
                    if ("-" != dr[i].ToString().Trim() && "" != dr[i].ToString().Trim())
                    {
                        InsertChildren(
                           dr[i].ToString().Trim(),
                           id);
                    }
                }
                //education 
                for (int i = 22; i <= 27; i++)
                {
                    if ("-" != dr[i].ToString().Trim() && "" != dr[i].ToString().Trim())
                    {
                        InsertEducation(
                           dr[i].ToString().Trim(),
                           id);
                    }
                }
                //acheivment
                for (int i = 28; i <= 28; i++)
                {
                    if ("-" != dr[i].ToString().Trim() && "" != dr[i].ToString().Trim())
                    {
                        InsertAcheivment(
                           dr[i].ToString().Trim(),
                           id);
                    }
                }
                //InsertWorkingExperience
                for (int i = 29; i <= 43; i++)
                {
                    if ("-" != dr[i].ToString().Trim() && "" != dr[i].ToString().Trim())
                    {
                        InsertWorkingExperience(
                           dr[i].ToString().Trim(),
                           id);
                    }
                }
                //polytical affliation
                for (int i = 44; i <= 47; i++)
                {
                    if ("-" != dr[i].ToString().Trim() && "" != dr[i].ToString().Trim())
                    {
                        InsertPoliticalkAffliation(
                        GetReferencePoliticalID(dr[i].ToString().Trim()),

                           id);
                    }
                }
                //InsertOtherOrganisationaffliation
                for (int i = 48; i <= 51; i++)
                {
                    if ("-" != dr[i].ToString().Trim() && "" != dr[i].ToString().Trim())
                    {
                        InsertOtherOrganisationaffliation(
                           dr[i].ToString().Trim(),
                           id);
                    }
                }

                //Internal PIC //57
                for (int i = 54; i <= 54; i++)
                {
                    if ("-" != dr[i].ToString().Trim() && "" != dr[i].ToString().Trim())
                    {
                        InsertInternalPic(
                           dr[i].ToString().Trim(),
                           dr[i + 1].ToString().Trim(),
                           dr[i + 2].ToString().Trim(),
                           dr[i + 3].ToString().Trim(),
                           id);
                    }
                }

                //stakeholdernetwork
                for (int i = 58; i <= 70; i++)
                {
                    if ("-" != dr[i].ToString().Trim() && "" != dr[i].ToString().Trim())
                    {
                        InsertStakeholderNetwork(
                           dr[i].ToString().Trim(),
                           dr[i + 1].ToString().Trim(),
                           dr[i + 2].ToString().Trim(),
                           dr[i + 3].ToString().Trim(),
                           id);
                    }
                }

                //LatestStakeHolderPerception--pending ---since it related to stategic objective & Area of Concerns pending
                //for (int i = 69; i <= 69; i++)
                //{
                //    if ("-" != dr[i].ToString().Trim() && "" != dr[i].ToString().Trim())
                //    {
                //        LatestStakeHolderPerception(
                //           dr[i].ToString().Trim(),
                //           dr[i + 1].ToString().Trim(),
                //           id);
                //    }
                //}

                //Individual Engagementrecord
                if ("-" != dr[76].ToString().Trim() && "" != dr[76].ToString().Trim())
                {
                    InsIndividualProfileEngRecord(
                        DateTime.Parse(dr[76].ToString().Trim()),
                        dr[77].ToString().Trim(),
                        GetReferenceEngagementTypeId(dr[78].ToString().Trim()),
                        id
                        );
                }
                //source
                for (int i = 89; i <= 91; i++)
                {
                    if ("-" != dr[i].ToString().Trim() && "" != dr[i].ToString().Trim())
                    {
                        InsertSource(
                            dr[i].ToString().Trim(),
                            id);
                    }
                }


            }
            #endregion
        }

        //ReadExcel
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

        //Smetindividual
        private void InsInvProfiles(Int64 id, Int64 func_id, string indv_stakeholder_name, string indv_name, string indv_title, string indv_birthdate, string indv_address, string indv_cell_number, string indv_email1, string indv_spouse_name, string indv_personal_interest, string indv_personal_preferences, string indv_stakeholder_responsibility, string indv_remarks, string staus_puplish)
        {
            string _query;
            if (IsDate(indv_birthdate))
            {
                _query = "INSERT INTO [indv_profiles] ([id],[func_id] ,[indv_stakeholder_name] ,[indv_name] ,[indv_title],[indv_birthdate],[indv_address] ,[indv_cell_number] ,[indv_email1],[indv_spouse_name],[indv_personal_interest],[indv_personal_preferences],[indv_stakeholder_responsibility],[indv_remarks],[staus_puplish]) VALUES (@id, @param1, @param2, @param3, @param4, @param5, @param6, @param7, @param8, @param9, @param10,@param11,@param12,@param13,@param14)";
            }
            else
            {
                _query = "INSERT INTO [indv_profiles] ([id],[func_id] ,[indv_stakeholder_name] ,[indv_name] ,[indv_title],[indv_address] ,[indv_cell_number] ,[indv_email1],[indv_spouse_name],[indv_personal_interest],[indv_personal_preferences],[indv_stakeholder_responsibility],[indv_remarks],[staus_puplish]) VALUES (@id, @param1, @param2, @param3, @param4,  @param6, @param7, @param8, @param9, @param10,@param11,@param12,@param13,@param14)";
            }
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
                        cmd.Parameters.Add("@param3", SqlDbType.VarChar, 255).Value = indv_name;
                        cmd.Parameters.Add("@param4", SqlDbType.VarChar, 255).Value = indv_title;
                        if (IsDate(indv_birthdate))
                        {
                            cmd.Parameters.Add("@param5", SqlDbType.Date).Value = DateTime.Parse(indv_birthdate);
                        }
                        cmd.Parameters.Add("@param6", SqlDbType.VarChar, 255).Value = indv_address;
                        //cmd.Parameters.Add("@param7", SqlDbType.Date).Value = indv_birthdate;
                        cmd.Parameters.Add("@param7", SqlDbType.VarChar, 255).Value = indv_cell_number;
                        cmd.Parameters.Add("@param8", SqlDbType.VarChar, 255).Value = indv_email1;
                        cmd.Parameters.Add("@param9", SqlDbType.VarChar, 255).Value = indv_spouse_name;
                        cmd.Parameters.Add("@param10", SqlDbType.VarChar, 255).Value = indv_personal_interest;
                        cmd.Parameters.Add("@param11", SqlDbType.VarChar, 255).Value = indv_personal_preferences;
                        cmd.Parameters.Add("@param12", SqlDbType.VarChar, 255).Value = indv_stakeholder_responsibility;
                        cmd.Parameters.Add("@param13", SqlDbType.VarChar, 255).Value = indv_remarks;
                        cmd.Parameters.Add("@param14", SqlDbType.Date).Value = staus_puplish;

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

        //source
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

        //GetReferenceEngagementTypeId
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

        //education
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

        //IsDate
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

        //InsertAcheivment
        private void InsertAcheivment(string indv_achievement, Int64 indv_id)
        {
            string _query = "INSERT INTO [indv_profiles_achievement]([indv_achievement],[indv_id]) VALUES (@param1,@indv_id)";
            using (SqlConnection sqlConn = new SqlConnection(_sConnStr))
            {
                sqlConn.Open();
                using (SqlCommand cmd = new SqlCommand(_query, sqlConn))
                {
                    try
                    {
                        cmd.Parameters.Add("@param1", SqlDbType.VarChar, 255).Value = indv_achievement;
                        cmd.Parameters.Add("@indv_id", SqlDbType.BigInt).Value = indv_id;
                        cmd.CommandType = CommandType.Text;
                        cmd.ExecuteNonQuery();
                    }
                    catch (Exception Ex)
                    { throw Ex; }
                }

            }
        }

        //working Experience
        private void InsertWorkingExperience(string indv_achievement, Int64 indv_id)
        {
            string _query = "INSERT INTO [indv_profiles_achievement]([indv_achievement],[indv_id]) VALUES (@param1,@indv_id)";
            using (SqlConnection sqlConn = new SqlConnection(_sConnStr))
            {
                sqlConn.Open();
                using (SqlCommand cmd = new SqlCommand(_query, sqlConn))
                {
                    try
                    {
                        cmd.Parameters.Add("@param1", SqlDbType.VarChar, 255).Value = indv_achievement;
                        cmd.Parameters.Add("@indv_id", SqlDbType.BigInt).Value = indv_id;
                        cmd.CommandType = CommandType.Text;
                        cmd.ExecuteNonQuery();
                    }
                    catch (Exception Ex)
                    { throw Ex; }
                }

            }
        }

        //children
        private void InsertChildren(string chld_name, Int64 indv_id)
        {
            string _query = "INSERT INTO [indv_profiles_children]([chld_name],[indv_id]) VALUES (@param1,@indv_id)";
            using (SqlConnection sqlConn = new SqlConnection(_sConnStr))
            {
                sqlConn.Open();
                using (SqlCommand cmd = new SqlCommand(_query, sqlConn))
                {
                    try
                    {
                        cmd.Parameters.Add("@param1", SqlDbType.VarChar, 255).Value = chld_name;
                        cmd.Parameters.Add("@indv_id", SqlDbType.BigInt).Value = indv_id;
                        cmd.CommandType = CommandType.Text;
                        cmd.ExecuteNonQuery();
                    }
                    catch (Exception Ex)
                    { throw Ex; }
                }

            }
        }

        //InsertPoliticalkAffliation
        private void InsertPoliticalkAffliation(int pltc_id, Int64 indv_id)
        {
            string _query = "INSERT INTO [indv_profiles_politicalaffiliation]([pltc_id],[indv_id]) VALUES (@param1,@indv_id)";
            using (SqlConnection sqlConn = new SqlConnection(_sConnStr))
            {
                sqlConn.Open();
                using (SqlCommand cmd = new SqlCommand(_query, sqlConn))
                {
                    try
                    {
                        cmd.Parameters.Add("@param1", SqlDbType.VarChar, 255).Value = pltc_id;
                        cmd.Parameters.Add("@indv_id", SqlDbType.BigInt).Value = indv_id;
                        cmd.CommandType = CommandType.Text;
                        cmd.ExecuteNonQuery();
                    }
                    catch (Exception Ex)
                    { throw Ex; }
                }

            }
        }

        //InsIndividualProfileEngRecord
        private int InsIndividualProfileEngRecord(DateTime engr_date, string engr_topic, Int32 engt_id, Int64 indv_id)
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

        //OtherOrganisation
        private void InsertOtherOrganisationaffliation(string ooga_name, Int64 indv_id)
        {
            string _query = "INSERT INTO [indv_profiles_otherorgzaffiliation]([ooga_name],[indv_id]) VALUES (@param1,@indv_id)";
            using (SqlConnection sqlConn = new SqlConnection(_sConnStr))
            {
                sqlConn.Open();
                using (SqlCommand cmd = new SqlCommand(_query, sqlConn))
                {
                    try
                    {
                        cmd.Parameters.Add("@param1", SqlDbType.VarChar, 255).Value = ooga_name;
                        cmd.Parameters.Add("@indv_id", SqlDbType.BigInt).Value = indv_id;
                        cmd.CommandType = CommandType.Text;
                        cmd.ExecuteNonQuery();
                    }
                    catch (Exception Ex)
                    { throw Ex; }
                }

            }
        }

        //getReference political Id
        private int GetReferencePoliticalID(string _sReferencePositionName)
        {
            string query = "SELECT [id] FROM [reff_political] WHERE [value] = @param1";

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

        //InsertInternalPic
        private void InsertInternalPic(string intc_name, string intc_division, string intc_email, string intc_phonenumber, Int64 indv_id)
        {
            string _query = "INSERT INTO [indv_profiles_internalcontact] ([intc_name],[intc_division],[intc_email],[intc_phonenumber],[indv_id]) VALUES (@param1,@param2,@param3,@param4,@indv_id)";
            using (SqlConnection sqlConn = new SqlConnection(_sConnStr))
            {
                sqlConn.Open();
                using (SqlCommand cmd = new SqlCommand(_query, sqlConn))
                {
                    try
                    {
                        cmd.Parameters.Add("@param1", SqlDbType.VarChar, 255).Value = intc_name;
                        cmd.Parameters.Add("@param2", SqlDbType.VarChar, 255).Value = intc_division;
                        cmd.Parameters.Add("@param3", SqlDbType.VarChar, 255).Value = intc_email;
                        cmd.Parameters.Add("@param4", SqlDbType.VarChar, 255).Value = intc_phonenumber;
                        cmd.Parameters.Add("@indv_id", SqlDbType.BigInt).Value = indv_id;
                        cmd.CommandType = CommandType.Text;
                        cmd.ExecuteNonQuery();
                    }
                    catch (Exception Ex)
                    { throw Ex; }
                }
            }
        }

        //InsertStakeholderNetwork
        private void InsertStakeholderNetwork(string shnk_name, string shnk_organization_name, string shnk_email, string shnk_phonenumber, Int64 indv_id)
        {
            string _query = "INSERT INTO [indv_profiles_stakeholdernetwork] ([shnk_name],[shnk_organization_name],[shnk_email],[shnk_phonenumber],[indv_id]) VALUES (@param1,@param2,@param3,@param4,@indv_id)";
            using (SqlConnection sqlConn = new SqlConnection(_sConnStr))
            {
                sqlConn.Open();
                using (SqlCommand cmd = new SqlCommand(_query, sqlConn))
                {
                    try
                    {
                        cmd.Parameters.Add("@param1", SqlDbType.VarChar, 255).Value = shnk_name;
                        cmd.Parameters.Add("@param2", SqlDbType.VarChar, 255).Value = shnk_organization_name;
                        cmd.Parameters.Add("@param3", SqlDbType.VarChar, 255).Value = shnk_email;
                        cmd.Parameters.Add("@param4", SqlDbType.VarChar, 255).Value = shnk_phonenumber;
                        cmd.Parameters.Add("@indv_id", SqlDbType.BigInt).Value = indv_id;
                        cmd.CommandType = CommandType.Text;
                        cmd.ExecuteNonQuery();
                    }
                    catch (Exception Ex)
                    { throw Ex; }
                }
            }
        }

        //LatestStakeHolderPerception
        private void LatestStakeHolderPerception(int prcp_id, int stob_id, Int64 indv_id)
        {
            string _query = "INSERT INTO [indv_profiles_latestperception] ([prcp_id],[stob_id],[indv_id]) VALUES (@param1,@param2,@indv_id)";
            using (SqlConnection sqlConn = new SqlConnection(_sConnStr))
            {
                sqlConn.Open();
                using (SqlCommand cmd = new SqlCommand(_query, sqlConn))
                {
                    try
                    {
                        cmd.Parameters.Add("@param1", SqlDbType.Int).Value = prcp_id;
                        cmd.Parameters.Add("@param2", SqlDbType.Int).Value = stob_id;
                        cmd.Parameters.Add("@indv_id", SqlDbType.BigInt).Value = indv_id;
                        cmd.CommandType = CommandType.Text;
                        cmd.ExecuteNonQuery();
                    }
                    catch (Exception Ex)
                    { throw Ex; }
                }
            }
        }


        //Extras
        public bool IsNumeric(string input)
        {
            int test;
            return int.TryParse(input, out test);
        }

        private void dataload(Int64 id, Int64 func_id, string organisation, string name, string title, DateTime birthdate, string address, string phonenumber, string email, string spousename, string personalintreast, string personalreference, string stakeresponse, string remarks, string updatestatus)
        {
            string _query = "INSERT INTO [indv_profiles] ([id],[func_id] ,[indv_stakeholder_name] ,[indv_stakeholder_name_others] ,[indv_stakeholder_group],[indv_bondholder] ,[indv_name],[indv_address] ,[indv_cell_number],[indv_office_number],[indv_email1],[indv_email2],[staus_puplish],[stht_id]) VALUES (@id, @param1, @param2, @param3, @param4, @param5, @param6, @param7, @param8, @param9, @param10,@param11,@param12,@param13,@param14)";
            using (SqlConnection sqlConn = new SqlConnection(_sConnStr))
            {
                sqlConn.Open();
                using (SqlCommand cmd = new SqlCommand(_query, sqlConn))
                {
                    try
                    {
                        cmd.Parameters.Add("@id", SqlDbType.Int).Value = id;
                        cmd.Parameters.Add("@param1", SqlDbType.Int).Value = func_id;
                        cmd.Parameters.Add("@param2", SqlDbType.VarChar, 255).Value = organisation;
                        cmd.Parameters.Add("@param3", SqlDbType.VarChar, 255).Value = name;
                        cmd.Parameters.Add("@param4", SqlDbType.VarChar, 255).Value = title;
                        cmd.Parameters.Add("@param5", SqlDbType.Date).Value = birthdate;

                        cmd.Parameters.Add("@param6", SqlDbType.VarChar, 255).Value = address;
                        //cmd.Parameters.Add("@param7", SqlDbType.Date).Value = indv_birthdate;
                        cmd.Parameters.Add("@param7", SqlDbType.VarChar, 255).Value = phonenumber;
                        cmd.Parameters.Add("@param8", SqlDbType.VarChar, 255).Value = email;
                        cmd.Parameters.Add("@param9", SqlDbType.VarChar, 255).Value = spousename;
                        cmd.Parameters.Add("@param10", SqlDbType.VarChar, 255).Value = personalintreast;
                        cmd.Parameters.Add("@param11", SqlDbType.VarChar, 255).Value = personalreference;
                        cmd.Parameters.Add("@param12", SqlDbType.Date).Value = stakeresponse;
                        cmd.Parameters.Add("@param13", SqlDbType.Int).Value = remarks;
                        cmd.Parameters.Add("@param14", SqlDbType.VarChar, 255).Value = updatestatus;

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
