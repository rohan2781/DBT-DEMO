select
		cast(null as STRING) as Bill_no,
		receipt_detail.receipt_date as actual_date,
		'R' as row_type,
		ifnull(office_master.srno,0) as office_id,
		ifnull(office_master.office_code,'All') as office_code,
		ifnull(office_master.name,'All') as office,
		ifnull(emp_master.empid,0) as doctor_id,
		case when emp_master.empid is null then 'All'
			else concat(emp_master.lastname,', ',emp_master.firstname) end as provider,
		receipt_detail.doctor_id as perf_doctor_id,
		case when emp_master.empid is null then 'All' else concat(emp_master.lastname,', ',emp_master.firstname) End as performing_provider,
		cast(case when receipt_detail.paid_by = 'I' then 
			case receipt_detail.ins_type 
				when 'A' then '0'
				when 'C' then cast(receipt_detail.insurance_carrier_id as STRING)
				when 'P' then cast(insurance_plan_master.carrier_id as STRING)
                else '0'
			end
            else '0'
		end as INTEGER) as carrier_id,
		case when receipt_detail.paid_by = 'P' then 'Patient Paid'
		else ifnull(insurance_carrier_master.name,'All Insurance Carrier')
		end insurance_carrier,
		cast(case when receipt_detail.paid_by = 'I' then 
			case receipt_detail.ins_type 
				when 'A' then '0' 
				when 'C' then '0'
				when 'P' then cast(receipt_detail.insurance_id as STRING)
                else '0'
			end
            else '0'
		end as INTEGER) as plan_id,
		case when receipt_detail.paid_by = 'P' then 'Patient Paid'
		else ifnull(insurance_plan_master.name,'All Insurance Plan')
		end insurance_plan,
		patient_master.ref_party_id as ref_party_id,
		patient_master.referral_source as referral_source,
		patient_master.patient_type as patient_type,
		patient_master.patient_type_description as patient_type_description,
		cast(null as STRING) as	Bill_Created_by,
		cast(null as STRING) as CPT_Created_by,
		cast(null as Date) as BillHeader_Created_Date,
		cast(null as Date) as BillDetail_Created_Date,
		cast(null as STRING) as billing_id,
		cast(null as STRING) as CPT_Descr,
		0 as patient_count,
		0 as patient_visit_count,
		0 as CPT_Count,
		0 as charges,
		0 as adjustment_plus,
		0 as allowed_amount,
		sum(receipt_detail.amount) as gross_revenue,
		0 as pat_write_off,
		0 as Ins_write_off,
		0 as total_write_off,
		sum(receipt_detail.refund_amount) as refund,
		0 as open_credit,
		sum(case when receipt_detail.payment_type = 'A' then 0 else receipt_subdetail.over_paid end ) as over_paid,
		sum(Case receipt_detail.payment_type when 'A' Then 0 Else Case paid_by when 'I' then receipt_detail.unapply_amount Else 0 End End) RMP,
		sum(case when receipt_detail.payment_type = 'A' then ( ifnull(cast(receipt_detail.unapply_amount as NUMERIC),0) - ifnull(cast(receipt_detail.refund_amount as NUMERIC),0) ) + receipt_subdetail.amount else 0 end ) as adjustment_negative,
		0 as pat_balance_due,
		0 as ins_balance_due,
		0 as total_balance_due,
		0 as ar_30,
		0 as ar_30_60,
		0 as ar_60_90,
		0 as ar_90,
		0 as pat_net_revenue,
		0 as ins_net_revenue,
		0 as total_net_revenue
	from {{ ref('Temp_Receipt_Detail') }} as receipt_detail 
	left join `titanium-atlas-428808-e7`.`DBT_Dataset`.`Office_Master` as office_master on receipt_detail.office_id = office_master.srno
	left join (
		select  tran_id,
				sum(case when cast(receipt_subdetail.op_type as INTEGER) = 1 then ifnull(cast(receipt_subdetail.amount as NUMERIC),0) - ifnull(cast(receipt_subdetail.refund_amount as NUMERIC),0) else 0 end ) as over_paid,
				sum(receipt_subdetail.amount) as amount 
		from `titanium-atlas-428808-e7`.`DBT_Dataset`.`Receipt_Subdetail` as receipt_subdetail
		group by tran_id ) as receipt_subdetail 
		on receipt_subdetail.tran_id = receipt_detail.receipt_id
	left join `titanium-atlas-428808-e7`.`DBT_Dataset`.`Emp_Master` as emp_master on receipt_detail.doctor_id = emp_master.empid 
	left join `titanium-atlas-428808-e7`.`DBT_Dataset`.`Insurance_Plan_Master` as insurance_plan_master on cast(case when receipt_detail.paid_by = 'I' then case receipt_detail.ins_type when 'A' then '0' when 'C' then '0' when 'P' then cast(receipt_detail.insurance_id as STRING)  else '0' end else '0' end as INTEGER) = cast(insurance_plan_master.srno as INTEGER)
	left join `titanium-atlas-428808-e7`.`DBT_Dataset`.`Insurance_Carrier_Master` as insurance_carrier_master on cast(case when receipt_detail.paid_by = 'I' then case receipt_detail.ins_type when 'A' then '0' when 'C' then cast(receipt_detail.insurance_carrier_id as STRING) when 'P' then cast(insurance_plan_master.carrier_id as STRING)  else '0' end else '0' end as INTEGER) = cast(insurance_carrier_master.srno as INTEGER)
	left join (
			select 
				patient_master.id as patient_id,
				patient_master.patient_no as patient_no,
				concat(patient_master.lastname,'', '',patient_master.firstname) as patient,
				patient_type_master.patient_type as patient_type,
				patient_type_master.description as patient_type_description,
				referral_party_master.srno as ref_party_id,
				referral_party_master.name as referral_source,
				row_number() over (partition by patient_master.id order by patient_type_detail.srno) as rowid
			from `titanium-atlas-428808-e7`.`DBT_Dataset`.`Patient_Master` as patient_master 
			left join `titanium-atlas-428808-e7`.`DBT_Dataset`.`Referral_Party_Master` as referral_party_master on patient_master.referral_party_id = referral_party_master.srno
			left join `titanium-atlas-428808-e7`.`DBT_Dataset`.`Patient_Type_Detail` as patient_type_detail on patient_type_detail.patient_id = patient_master.id
			left join `titanium-atlas-428808-e7`.`DBT_Dataset`.`Patient_Type_Master` as patient_type_master on patient_type_detail.patient_type = patient_type_master.patient_type
		) as patient_master on receipt_detail.patient_id = patient_master.patient_id 
		and receipt_detail.paid_by = 'P'
		and patient_master.rowid = 1
	group by row_type,receipt_date,office_id,office_code,office,doctor_id,provider,perf_doctor_id,performing_provider,carrier_id,insurance_carrier,plan_id,insurance_plan,ref_party_id,referral_source,patient_type,patient_type_description,billing_id,CPT_Descr,Bill_no,Bill_Created_by,CPT_Created_by,BillHeader_Created_Date,BillDetail_Created_Date