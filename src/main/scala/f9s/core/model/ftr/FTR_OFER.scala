package f9s.core.model.ftr


import scala.beans.BeanProperty


case class FTR_OFER(
                     @BeanProperty var ID: Long,
                     @BeanProperty var CRE_USR_ID: String,
                     @BeanProperty var UPD_USR_ID: String,
                     @BeanProperty var ALL_YN: String,
                     @BeanProperty var DEAL_CHNG_SEQ: Long,
                     @BeanProperty var DEAL_NR: String,
                     @BeanProperty var DEL_DT: String,
                     @BeanProperty var DEL_YN: String,
                     @BeanProperty var EFCT_DT: String,
                     @BeanProperty var EMP_NR: String,
                     @BeanProperty var EXPR_YN: String,
                     @BeanProperty var INPT_SRC: String,
                     @BeanProperty var MSTR_CTRK_NR: String,
                     @BeanProperty var OFER_CNCL_REQ_DT: String,
                     @BeanProperty var OFER_CHNG_SEQ: String,
                     @BeanProperty var OFER_DT: String,
                     @BeanProperty var OFER_MAIN_STS_CD: String,
                     @BeanProperty var OFER_NR: String,
                     @BeanProperty var OFER_PYMT_TRM_CD: String,
                     @BeanProperty var OFER_RD_TRM_CD: String,
                     @BeanProperty var OFER_REJT_CD: String,
                     @BeanProperty var OFER_TX_TP_CD: String,
                     @BeanProperty var OFER_TP_CD: String,
                     @BeanProperty var REF_OFER_CHNG_SEQ: Long,
                     @BeanProperty var REF_OFER_NR: String,
                     @BeanProperty var TIME_IN_FRC_CD: String,
                     @BeanProperty var TRDE_CO_CD: String,
                     @BeanProperty var TRDE_MKT_TP_CD: String,
                     @BeanProperty var TRDE_ROLE_CD: String,
                     @BeanProperty var VT_GRP_AGRE_YN: String,
                     @BeanProperty var VT_GRP_OFER_YN: String,
                     @BeanProperty var WIF_ARGE_YN: String) {

}