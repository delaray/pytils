SELECT om.num_cli as clientCode, om.id_cmde_web as webOrderCode,
omd.num_art as productCode, mnt_total as productTotalCost,
cod_status_proj as orderStatus, num_vendeur as vendorCode,
nom_vendeur as vendorName, nbr_qte as quantity, is_3P as status3P,
omd.dat_cre as orderDate, omd.dat_expedition as shippingDate
FROM `lmfr-ddp-dwh-prd.web_order.vf_order_merged` as om
INNER JOIN `lmfr-ddp-dwh-prd.web_order.vf_order_detail_merged`  as omd
ON om.id_cmde_web = omd.id_cmde_web
WHERE dat_cre >= '2022-05-01'
AND dat_cre <= '2022-05-31'
