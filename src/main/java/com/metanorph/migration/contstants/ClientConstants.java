package com.metanorph.migration.contstants;

import org.springframework.http.MediaType;
import java.util.regex.Pattern;

public class ClientConstants {

    private ClientConstants() {}

    public static final String CLIENT_TABLE = "CLIENT";
    public static final String CLIENT_GUID = "client_guid";
    public static final String ID = "id";
    public static final String CLIENT_TYPE_CD = "clientTypeCd";
    public static final MediaType EXCEL_MEDIA_TYPE = MediaType.parseMediaType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
    public static final Pattern SAFE_SQL_IDENTIFIER = Pattern.compile("^[A-Za-z]\\w*$");
    public static final String SQL_SERVER_ACCESS_DENIED_FRAGMENT = "Cannot open database";
    public static final int SQL_SERVER_CANNOT_OPEN_DATABASE_ERROR = 4060;
    public static final String CLAIM_HISTORY_CLIENT_REF_PRIMARY = "claimHistoryClientRefPrimary";
    public static final String CLAIM_HISTORY_CLIENT_REF_SECONDARY = "claimHistoryClientRefSecondary";
    public static final String CLIENT_REF_GUID = "clientRefGuid";
    public static final String CLAIM_HISTORY_REF_GUID  = "claim_history_ref_guid";
    public static final String CLIENT_GUID_LINK = "clientGuid";
    public static final String CLAIM_GUID_LINK = "claimGuid";
    public static final String PERSON_CLIENT_TYPE = "PERSON";
    public static final String STATUS_CD = "statusCd";
    public static final String INSURED = "INSURED";
    public static final String CLAIM_HISTORY_CLIENT_TABLE = "CLAIM_HISTORY_CLIENT";
    public static final String CLIENT_ADDRESS_TABLE = "CLIENT_ADDRESS";
    public static final String CLIENT_PHONE_TABLE = "CLIENT_PHONE";
    public static final String CLIENT_EMAIL_TABLE = "CLIENT_EMAIL";
    public static final String CLIENT_RELATIONSHIP_TABLE = "CLIENT_RELATIONSHIP";
    public static final String CLIENT_ADD_FLD_TABLE = "CLIENT_ADD_FLD";
    public static final String PERSON_TABLE = "PERSON";
    public static final String CLAIM_HISTORY_PAYMENT_TABLE = "CLAIM_HISTORY_PAYMENT";
    public static final String PERSONAL_EMAIL_TYPE_CD = "PERSONAL";
    public static final String ROLE_CD = "roleCd";
    public static final String TYPE_CD = "typeCd";
    public static final String CLIENT_CD = "clientCd";
    public static final String ROLE_NOMINEE = "NOMINEE";
    public static final String ROLE_CLAIMANT = "CLAIMANT";
    public static final String ROLE_APPOINTEE = "APPOINTEE";
    public static final String CSV_COL_ADDRESS_LINE_1 = "TPCR_ADDRESSLINE1";
    public static final String CSV_COL_MOBILE_NUMBER = "TPCR_MOBILENUMBER";
    public static final String CSV_COL_EMAIL = "TPCR_EMAIL";
    public static final String CSV_COL_RELATION = "TPCR_RELATION";
    public static final String CSV_COL_CLAIMANT_NAME = "TPCR_CLAIMANT_NAME";
    public static final String CSV_COL_GUARDIAN_NAME = "TPCR_NAMEOFGUARDIAN";
    public static final String CSV_COL_GENDER = "TPCR_GENDER";
    public static final String CSV_COL_CLAIMANT_GENDER = "TPCR_CLAIMANTGENDER";
    public static final String CSV_COL_NOMINEE_GENDER = "TPCR_NOMINEEGENDER";
    public static final String CSV_COL_DOB_DECEASED = "TPCR_DOB_DECEASED";
    public static final String CSV_COL_CLAIMANT_DOB = "TPCR_CLAIMANT_DOB";
    public static final String CSV_COL_NOM_DOB = "TPCR_NOM_DOB";
    public static final String RELATIONSHIP_CD_PREFIX = "relationshipCd.";
    public static final String ROLE_GUARDIAN = "GUARDIAN";
    public static final String CLAIM_HISTORY_REF_GUID_COL = "claim_history_ref_guid";
    public static final String PERSON_IDENTITY_TABLE = "PERSON_IDENTITY";
    public static final String PERSON_GUID_COL = "personGuid";
    public static final String CLAIM_HISTORY_POLICY_TABLE = "CLAIM_HISTORY_POLICY";
    public static final String CLAIM_HISTORY_COVERAGE_TABLE = "CLAIM_HISTORY_COVERAGE";
    public static final String CLAIM_HISTORY_COV_BENEFIT_TABLE = "CLAIM_HISTORY_COV_BENEFIT";
    public static final String CLAIM_ADDITIONAL_FIELD_TABLE = "CLAIM_ADDITIONAL_FIELD";
    public static final String PLAN_CD = "planCd";
    public static final String APPROVED_AMT_CURRENCY = "approvedAmtPolCurrency";
    public static final String BENEFIT_CD = "benefitCd";
    public static final String RELATIONSHIP_CD = "relationshipCd";
    public static final String FIELD_KEY = "fieldKey";
    public static final String FIELD_VALUE = "fieldValue";
    public static final String CLAIM_HISTORY_CAUSE_OF_DEATH_CD = "causeOfDeathCd";
    public static final String CLAIM_HISTORY_EVENT_SUB_TYPE_CD = "eventSubTypeCd";
    public static final String CLAIM_HISTORY = "CLAIM_HISTORY";
    public static final String CLIENT_GUID_TO = "clientGuidTo";
    public static final String NATURAL = "NATURAL";
    public static final String CLIENT_NUM = "clientNum";
    public static final String CLIENT_NUM_TO_COL = "clientNumTo";
    public static final String DATE_OF_BIRTH = "dateOfBirth";
    public static final String DATE_FORMAT = "dd/MM/yyyy";
    public static final String TPCR_CLAIM_DECSN_CD = "TPCR_CLAIM_DECSN";
    public static final String CLAIM_HISTORY_SETTLEMENT = "CLAIM_HISTORY_SETTLEMENT";

    // ADMIT Sheet Name Mappings
    public static final String ADMIT_CLAIM_HISTORY = "CLM_INTIMATION";
    public static final String ADMIT_CLAIM_HISTORY_CLIENT = "CLM_INT_CLIENT";
    public static final String ADMIT_CLAIM_HISTORY_POLICY = "CLM_INT_POLICY";
    public static final String ADMIT_CLAIM_ADDITIONAL_FIELD = "INTIMATION_ADDITIONAL_FIELD";
    public static final String ADMIT_CLAIM_HISTORY_PAYMENT = "CLM_INT_PAYMENT";

    // Processing Constants
    public static final String GETDATE_LITERAL = "GETDATE()";
    public static final String PAYMENT_DATE_FIELD = "PAYMENT_DATE";
    public static final String TASK_TABLE = "TASK";
    public static final String PAID_STATUS = "PAID";
    public static final String DE_COMPLETE_STATUS = "DE_COMPLETE";
    public static final String PENDING_MANUAL_ADJ = "PENDING_MANUAL_ADJ";
    public static final String ADMIT = "ADMIT";
}
