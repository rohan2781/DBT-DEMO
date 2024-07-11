select 
		patient_master.patient_id as Patient_Id,
        cast(Bill_no as STRING) as Bill_no,
		temp_billing_header.service_date as actual_date,
		'C' as row_type,
		temp_billing_header.office_id as office_id,
		office_master.office_code as office_code,
		office_master.name as office,
		temp_billing_header.doctor_id as doctor_id,
		concat(emp_master.lastname,', ',emp_master.firstname) as Provider,
		temp_billing_header.perf_provider_id as perf_doctor_id,
		concat(perf_emp_master.lastname,', ',perf_emp_master.firstname) as performing_provider,
		case when billing_subdetail.tran_id is null then 0 else ifnull(insurance_carrier_master.srno,0) end as carrier_id,
		case when billing_subdetail.tran_id is null then '(Zero Charge)' else ifnull(insurance_carrier_master.name,'Patient Responsible') end as insurance_carrier,
		case when billing_subdetail.tran_id is null then 0 else ifnull(insurance_plan_master.srno,0) end as plan_id,
		case when billing_subdetail.tran_id is null then '(Zero Charge)' else ifnull(insurance_plan_master.name,'Patient Responsible') end as insurance_plan,
		patient_master.ref_party_id as ref_party_id,
		patient_master.referral_source as referral_source,
		patient_master.patient_type as patient_type,
		patient_master.patient_type_description as patient_type_description,
		Bill_Created_by,
		CPT_Created_by,
		BillHeader_Created_Date,
		BillDetail_Created_Date,
		temp_billing_header.billing_id as billing_id,
		temp_billing_header.CPT_Descr as CPT_Descr,
		sum(case when ifnull(cast(billing_subdetail.line_rowid as INTEGER),1) = 1 and cast(temp_billing_header.patient_rowid as INTEGER) = 1
			then 1 else 0 end) as patient_count,
		sum(case when ifnull(cast(billing_subdetail.line_rowid as INTEGER),1) = 1 and cast(temp_billing_header.ref_rowid as INTEGER) = 1 
			then 1 else 0 end) as patient_visit_count,
		sum(case when billing_subdetail.tran_id is null then temp_billing_header.billing_qty
		else case when ifnull(temp_billing_header.ins_min_line_id,0) <> 0 
			then (case when cast(temp_billing_header.ins_min_line_id as INTEGER) = cast(billing_subdetail.line_id as INTEGER) then temp_billing_header.billing_qty end)
		else case when cast(temp_billing_header.min_line_id as INTEGER) = cast(billing_subdetail.line_id as INTEGER) then temp_billing_header.billing_qty 
		else 0.00
		end end end ) as CPT_Count,
		sum( case when cast(temp_billing_header.adjustment_bill as STRING) = 'Y' then 0.00 
		else 
			case when ifnull(temp_billing_header.ins_min_line_id,0) <> 0 
				then (case when temp_billing_header.ins_min_line_id = billing_subdetail.line_id then temp_billing_header.total_billing_amount end)
			else case when temp_billing_header.min_line_id = billing_subdetail.line_id then temp_billing_header.total_billing_amount 
			else 0.00
			end end 
		end) as Charges,
		sum( case when cast(temp_billing_header.adjustment_bill as STRING) = 'Y' then 
				case when billing_subdetail.line_id = ifnull(temp_billing_header.min_line_id,-2)
				then temp_billing_header.total_billing_amount 
				else 0.00
				end
		else 0 end) as adjustment_plus, 
		sum(case when ifnull(temp_billing_header.ins_min_line_id,0) <> 0 
			then (case when temp_billing_header.ins_min_line_id = billing_subdetail.line_id then temp_billing_header.allowed_amt end)
		else case when temp_billing_header.min_line_id = billing_subdetail.line_id then temp_billing_header.allowed_amt 
		else 0.00
		end end ) as allowed_amount,
		0 as gross_revenue,
		sum(case when billing_subdetail.line_id = -1 then billing_subdetail.adjusted_amt else 0 end) as pat_write_off,
		sum(case when billing_subdetail.line_id <> -1 then billing_subdetail.adjusted_amt else 0 end) as Ins_write_off,
		sum(case when billing_subdetail.line_id = -1 then billing_subdetail.adjusted_amt else 0 end) + sum(case when billing_subdetail.line_id <> -1 then billing_subdetail.adjusted_amt else 0 end) as total_write_off,
		0 as refund,
		sum(case when billing_subdetail.line_id = -1 then temp_billing_header.pat_open_credit else 0.00 end) as open_credit,
		0 as over_paid,
		0 as RMP,
		0 as adjustment_negative,
		sum(case when billing_subdetail.line_id = -1 and cast(temp_billing_header.is_pending as INTEGER) = 1 and temp_billing_header.pat_balance > 0 then
			billing_aging.balance 
		else 0 end) as pat_balance_due,
		sum(case when billing_subdetail.line_id <> -1 and temp_billing_header.resp_party_id = billing_subdetail.ins_srid
				and temp_billing_header.resp_party_type = billing_subdetail.resp_party_type 
				and cast(temp_billing_header.is_pending as INTEGER) = 1 and temp_billing_header.ins_balance > 0 then 
			temp_billing_header.ins_balance 
		else 0 end) as ins_balance_due,
		sum(case when billing_subdetail.line_id = -1 and cast(temp_billing_header.is_pending as INTEGER) = 1 and temp_billing_header.pat_balance > 0 then
			billing_aging.balance 
		else 0 end) + sum(case when billing_subdetail.line_id <> -1 and temp_billing_header.resp_party_id = billing_subdetail.ins_srid
				and temp_billing_header.resp_party_type = billing_subdetail.resp_party_type 
				and cast(temp_billing_header.is_pending as INTEGER) = 1 and temp_billing_header.ins_balance > 0 then 
			temp_billing_header.ins_balance 
		else 0 end) as total_balance_due,
		CASE 
            WHEN DATE_DIFF(CURRENT_DATE(), temp_billing_header.service_date, DAY) <= 30 THEN sum(case when billing_subdetail.line_id = -1 and cast(temp_billing_header.is_pending as INTEGER) = 1 and temp_billing_header.pat_balance > 0 then billing_aging.balance  else 0 end) + sum(case when billing_subdetail.line_id <> -1 and temp_billing_header.resp_party_id = billing_subdetail.ins_srid and temp_billing_header.resp_party_type = billing_subdetail.resp_party_type and cast(temp_billing_header.is_pending as INTEGER) = 1 and temp_billing_header.ins_balance > 0 then temp_billing_header.ins_balance else 0 end)
            ELSE 0 
        END AS ar_30,

        CASE 
            WHEN DATE_DIFF(CURRENT_DATE(), temp_billing_header.service_date, DAY) > 30 
                AND DATE_DIFF(CURRENT_DATE(), temp_billing_header.service_date, DAY) <= 60 THEN sum(case when billing_subdetail.line_id = -1 and cast(temp_billing_header.is_pending as INTEGER) = 1 and temp_billing_header.pat_balance > 0 then billing_aging.balance  else 0 end) + sum(case when billing_subdetail.line_id <> -1 and temp_billing_header.resp_party_id = billing_subdetail.ins_srid and temp_billing_header.resp_party_type = billing_subdetail.resp_party_type and cast(temp_billing_header.is_pending as INTEGER) = 1 and temp_billing_header.ins_balance > 0 then temp_billing_header.ins_balance else 0 end)
            ELSE 0 
        END AS ar_30_60,

        CASE 
            WHEN DATE_DIFF(CURRENT_DATE(), temp_billing_header.service_date, DAY) > 60 
                AND DATE_DIFF(CURRENT_DATE(), temp_billing_header.service_date, DAY) <= 90 THEN sum(case when billing_subdetail.line_id = -1 and cast(temp_billing_header.is_pending as INTEGER) = 1 and temp_billing_header.pat_balance > 0 then billing_aging.balance  else 0 end) + sum(case when billing_subdetail.line_id <> -1 and temp_billing_header.resp_party_id = billing_subdetail.ins_srid and temp_billing_header.resp_party_type = billing_subdetail.resp_party_type and cast(temp_billing_header.is_pending as INTEGER) = 1 and temp_billing_header.ins_balance > 0 then temp_billing_header.ins_balance else 0 end)
            ELSE 0 
        END AS ar_60_90,

        CASE 
            WHEN DATE_DIFF(CURRENT_DATE(), temp_billing_header.service_date, DAY) > 90 THEN sum(case when billing_subdetail.line_id = -1 and cast(temp_billing_header.is_pending as INTEGER) = 1 and temp_billing_header.pat_balance > 0 then billing_aging.balance  else 0 end) + sum(case when billing_subdetail.line_id <> -1 and temp_billing_header.resp_party_id = billing_subdetail.ins_srid and temp_billing_header.resp_party_type = billing_subdetail.resp_party_type and cast(temp_billing_header.is_pending as INTEGER) = 1 and temp_billing_header.ins_balance > 0 then temp_billing_header.ins_balance else 0 end)
            ELSE 0 
        END AS ar_90,
		sum(case when billing_subdetail.line_id = -1 then receipt_subdetail.amount else 0 end) as pat_net_revenue,
		sum(case when billing_subdetail.line_id <> -1 then receipt_subdetail.amount else 0 end) as ins_net_revenue,
		sum(case when billing_subdetail.line_id = -1 then receipt_subdetail.amount else 0 end) + sum(case when billing_subdetail.line_id <> -1 then receipt_subdetail.amount else 0 end) as total_net_revenue
	from  `titanium-atlas-428808-e7`.`DBT_Dataset`.`Temp_Billing_Header` as temp_billing_header
	join (
		select 
			patient_master.id as patient_id,
			patient_master.patient_no as patient_no,
			concat(patient_master.lastname,', ',patient_master.firstname) as patient,
			patient_type_master.patient_type as patient_type,
			patient_type_master.description as patient_type_description,
			referral_party_master.srno as ref_party_id,
			referral_party_master.name as referral_source,
			row_number() over (partition by patient_master.id order by patient_type_detail.srno) as rowid
		from `titanium-atlas-428808-e7`.`DBT_Dataset`.`Patient_Master` as patient_master 
		left join `titanium-atlas-428808-e7`.`DBT_Dataset`.`Referral_Party_Master` as referral_party_master on patient_master.referral_party_id = referral_party_master.srno
		left join `titanium-atlas-428808-e7`.`DBT_Dataset`.`Patient_Type_Detail` as patient_type_detail on patient_type_detail.patient_id = patient_master.id
		left join `titanium-atlas-428808-e7`.`DBT_Dataset`.`Patient_Type_Master` as patient_type_master on patient_type_detail.patient_type = patient_type_master.patient_type
	) as patient_master on temp_billing_header.patient_id = patient_master.patient_id and patient_master.rowid = 1
	left join (select case billing_subdetail.ins_category when 'I' then 'CI' when 'P' then 'PI' else 'P' end as resp_party_type,
					 row_number() over (partition by tran_id,sr_id order by (case when line_id = -1 then 999 else line_id end) ) as line_rowid,* 
		from `titanium-atlas-428808-e7`.`DBT_Dataset`.`Billing_Subdetail` as billing_subdetail) as billing_subdetail on temp_billing_header.tran_id = billing_subdetail.tran_id 
		and temp_billing_header.sr_id = billing_subdetail.sr_id
	left join (select srv_tran_id,srv_sr_id,line_id ,sum(receipt_subdetail.amount) as amount
		from `titanium-atlas-428808-e7`.`DBT_Dataset`.`Receipt_Subdetail` as receipt_subdetail 
		join `titanium-atlas-428808-e7`.`DBT_Dataset`.`Receipt_Detail` as receipt_detail on receipt_subdetail.tran_id = receipt_detail.receipt_id and receipt_detail.payment_type <> 'A'
		group by srv_tran_id,srv_sr_id,line_id ) as receipt_subdetail
		on billing_subdetail.tran_id = receipt_subdetail.srv_tran_id 
		   and billing_subdetail.sr_id = receipt_subdetail.srv_sr_id
		   and billing_subdetail.line_id = receipt_subdetail.line_id
	left join (select tran_id,sr_id,line_id,sum(billing_aging.amount-billing_aging.amt_adjusted) as balance 
	from `titanium-atlas-428808-e7`.`DBT_Dataset`.`Billing_Aging` as billing_aging
	where billing_aging.amount-billing_aging.amt_adjusted > 0
	group by tran_id,sr_id,line_id) as billing_aging on billing_subdetail.tran_id = billing_aging.tran_id 
		and billing_subdetail.sr_id = billing_aging.sr_id 
		and billing_subdetail.line_id = billing_aging.line_id 
	left join `titanium-atlas-428808-e7`.`DBT_Dataset`.`Office_Master` as office_master on office_id = office_master.srno
	left join `titanium-atlas-428808-e7`.`DBT_Dataset`.`Emp_Master` as emp_master on temp_billing_header.doctor_id = emp_master.empid
	left join `titanium-atlas-428808-e7`.`DBT_Dataset`.`Emp_Master` as perf_emp_master on temp_billing_header.perf_provider_id = perf_emp_master.empid
	left join `titanium-atlas-428808-e7`.`DBT_Dataset`.`Insurance_Plan_Master` as insurance_plan_master on billing_subdetail.insurance_id = insurance_plan_master.srno 
	left join `titanium-atlas-428808-e7`.`DBT_Dataset`.`Insurance_Carrier_Master` as insurance_carrier_master on insurance_plan_master.carrier_id = insurance_carrier_master.srno
	group by row_type,service_date,office_id,office_code,office,doctor_id,provider,perf_doctor_id,performing_provider,carrier_id,insurance_carrier,plan_id,insurance_plan,
		ref_party_id,referral_source,patient_type,patient_type_description,billing_id,CPT_Descr,Bill_NO,Bill_Created_by,CPT_Created_by,BillHeader_Created_Date,BillDetail_Created_Date,patient_master.patient_id