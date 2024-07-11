select 
		billing_header.tran_id as tran_id,
		billing_detail.sr_id as sr_id,
		billing_header.bill_no as bill_no,
		case when billing_detail.form_flag = 'U' then billing_detail.service_date else billing_header.service_from end as service_date,
		billing_header.total_amt as total_amount,
		billing_header.patient_id as patient_id,
		billing_header.ref_id as ref_id,
		billing_header.doctor_id as doctor_id,
        billing_header.perf_provider_id as perf_provider_id,
		billing_header.office_id as office_id,
		case when billing_detail.form_flag = 'U' then case_bill_detail.billing_id else billing_detail.billing_id end as billing_id,
		billing_master.descr as CPT_Descr,
		billing_detail.billing_amt as billing_amt,
		billing_detail.billing_qty as billing_qty,
		billing_header.adjustment_bill as adjustment_bill,
		billing_detail.amount as total_billing_amount,
		fee_schedule_template_sdt.approved_amt as allowed_amt,
		billing_detail.total_ins_paid as total_ins_paid,
		billing_detail.total_patient_paid as total_patient_paid,
		billing_detail.total_ins_writeoff as total_ins_writeoff,
		billing_detail.total_patient_writeoff as total_patient_writeoff,
		billing_detail.is_pending as is_pending,
		billing_detail.min_line_id as min_line_id,
		billing_detail.ins_min_line_id as ins_min_line_id,
		billing_detail.ins_balance as ins_balance,
		billing_detail.pat_balance as pat_balance,
		billing_detail.resp_party_id as resp_party_id,
		billing_detail.resp_party_type as resp_party_type,
		billing_detail.pat_open_credit as pat_open_credit,
		concat(emp_master.lastname,'', '',emp_master.firstname) as Bill_Created_by,
		concat(detail_emp_master.lastname,'', '',detail_emp_master.firstname) as CPT_Created_by,
		date(billing_header.created_date) as BillHeader_Created_Date,
		date(billing_detail.created_date) as BillDetail_Created_Date,
		row_number() over (partition by billing_header.tran_id order by billing_detail.sr_id) as rowid,
		row_number() over (partition by billing_header.ref_id order by billing_header.tran_id,billing_detail.sr_id) as ref_rowid,
		row_number() over (partition by billing_header.patient_id order by billing_header.tran_id,billing_detail.sr_id) as patient_rowid

	from `titanium-atlas-428808-e7`.`DBT_Dataset`.`Billing_Header` as billing_header
	join `titanium-atlas-428808-e7`.`DBT_Dataset`.`Billing_Detail` as billing_detail on billing_header.tran_id = billing_detail.tran_id 
	left join `titanium-atlas-428808-e7`.`DBT_Dataset`.`Emp_Master` as emp_master on billing_header.createdby_id = emp_master.empid 
	left join `titanium-atlas-428808-e7`.`DBT_Dataset`.`Emp_Master` as detail_emp_master on billing_detail.createdby_id = detail_emp_master.empid
	left join `titanium-atlas-428808-e7`.`DBT_Dataset`.`Case_Bill_Detail` as case_bill_detail on case_bill_detail.tran_id = billing_detail.tran_id 
		and case_bill_detail.sr_id = billing_detail.sr_id 
	join `titanium-atlas-428808-e7`.`DBT_Dataset`.`Billing_Master` as billing_master on ifnull(billing_detail.billing_Id,case_bill_detail.billing_id) = billing_master.billing_id 
	left join (
		select fee_schedule_Template_sdt.tran_id,fee_schedule_Template_sdt.cpt_code,fee_schedule_template_dtl.from_date,
			fee_schedule_template_dtl.to_date,approved_amt 
		from `titanium-atlas-428808-e7`.`DBT_Dataset`.`Fee_Schedule_Template_sdt` as fee_schedule_Template_sdt
		left outer join `titanium-atlas-428808-e7`.`DBT_Dataset`.`Fee_Schedule_Template_dtl` as fee_schedule_template_dtl on fee_schedule_template_dtl.tran_id = fee_schedule_Template_sdt.tran_id 
			and fee_schedule_template_dtl.sr_id=fee_schedule_Template_sdt.sr_id ) fee_Schedule_template_sdt 
		on fee_schedule_template_sdt.tran_id = billing_detail.fee_schedule_id 
		and fee_schedule_template_sdt.cpt_code= ifnull(billing_detail.billing_Id,case_bill_detail.billing_id)
        and ifnull(billing_detail.service_date,service_from) > fee_Schedule_template_sdt.from_Date 
		and ifnull(billing_detail.service_date,service_from) < fee_Schedule_template_sdt.to_Date