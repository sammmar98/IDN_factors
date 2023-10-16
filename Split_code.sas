/***********split*****************/
 data IDN.Split    ;
     %let _EFIERR_ = 0; /* set the ERROR detection macro variable */
      infile 'E:\Dropbox\Project_Indonesia Stock Market_Submission for PBFJ\Datasets\Data_StockSplit\datamatch_IDXSKEICompustat_StockSplit.txt' delimiter='09'x MISSOVER DSD lrecl=32767 firstobs=2 ;
        informat Code $4. ;
        informat GVKEY best32. ;
        informat ISIN $12. ;
        informat ActualDate best32. ;
        informat AnnounceDate best32. ;
        informat Stock_Splits $20. ;
        format Code $4. ;
        format GVKEY best12. ;
      format ISIN $12. ;
       format ActualDate best12. ;
        format AnnounceDate best12. ;
       format Stock_Splits $20.;
     input
        Code $
        GVKEY
        ISIN $
        ActualDate
        AnnounceDate
        Stock_Splits $
     ;
	if _ERROR_ then call symputx('_EFIERR_',1);  /* set ERROR detection macro variable */
run;

data IDN.Split;
	set IDN.Split;
	after=scan(Stock_Splits, 1, ':');
    before=scan(Stock_Splits, 2, ':');
run;

data IDN.Split;
	set IDN.Split;
   	after1 = input(compbl(after), 10.);
   	before1 = input(compbl(before), 10.);
run;

data IDN.Split;
	set IDN.Split;
	split_ratio=after1/before1;
	if split_ratio>=1 then split=1;
	else split=0; 
run;

data IDN.Split;
	set IDN.Split;
	if AnnounceDate=0 then AnnounceDate=.;
run;

data IDN.Split;
	set IDN.Split;
	DATE_ann = Input( Put( AnnounceDate, 8.), Yymmdd10.);
	DATE_act = Input( Put( ActualDate, 8.), Yymmdd10.);
	format DATE_ann date9.;
	format DATE_act date9.;
run;

data IDN.Split;
	set IDN.Split;
	begin=intnx('month', DATE_act, -13,'begin');
	end=intnx('month', DATE_act, 3,'same');
	format DATE_ann yymmddn8.;
	format DATE_act yymmddn8.;
	format begin yymmddn8.;
	format end yymmddn8.;
run;

data IDN.Split;
	set IDN.Split;
	begin_ann=intnx('month', DATE_ann, -13,'begin');
	end_ann=intnx('month', DATE_ann, 3,'same');
	format DATE_ann yymmddn8.;
	format DATE_act yymmddn8.;
	format begin_ann yymmddn8.;
	format end_ann yymmddn8.;
run;

data IDN.Split_ann;
	set IDN.Split;
	if not missing(DATE_ann);
run;
proc sql;
	create table idn.split as select a.*, b.sharia_stock from idn.split as a left join idn.cross_hold as b on a.gvkey=b.gvkey;
quit;

proc sort data=idn.split; by sharia_stock gvkey;run;

data idn.daily_idn;
	set idn.daily_idn;
	gvkey_n=input(gvkey, 6.);
run;


%macro split_aer(data,sub);
	proc sql;
		create table idn.split_spec as select a.*, b.DATE_ann, b.DATE_act, b.split 
	from idn.daily_idn as a, &data. as b where a.gvkey_n=b.gvkey and b.begin<=a.datadate<=b.end;
	quit;

	proc sort data=idn.split_spec; by gvkey DATE_act datadate; run;

	proc sort data=idn.split; by gvkey actualdate; run;


	data idn.split_spec;
	  set idn.split_spec;
	  count + 1;
	  by gvkey DATE_act;
	  if first.DATE_act then count = 1;
	run;

	data idn.split_spec_date;
		set idn.split_spec;
		if DATE_act=datadate;
	run;

	data idn.split_spec_date; 
		set idn.split_spec_date;
		a_start=count-250;
		a_end=count-120;
		w_start=count-61;
		w_end=count+60;
	run;

	data idn.split_spec_date; 
		set idn.split_spec_date;
		if a_end>=52;
		if a_start<0 then a_start=0;
	run; 

	proc sql;
		create table idn.split_spec as select a.*, b.a_start, b.a_end , b.w_start, b.w_end
	from idn.split_spec as a left join idn.split_spec_date as b on a.gvkey_n=b.gvkey_n and a.DATE_act=b.DATE_act;
	quit;

	proc sort data=idn.split_spec; by gvkey DATE_act datadate; run;

	data idn.split_reg;
		set idn.split_spec;
		if count>a_start and count<=a_end;
	run;

	proc reg data=idn.split_reg  outest=idn.split_ab noprint;
		by gvkey_n DATE_act;
		model ret_local = mkt_ret;
	run;

	data idn.split_ab;
		set idn.split_ab;
		if intercept^=0;

	run;

	proc sql;
		create table idn.split_spec as select a.*, b.intercept as a, b.mkt_ret as b
	from idn.split_spec as a left join idn.split_ab as b on a.gvkey_n=b.gvkey_n and a.DATE_act=b.DATE_act;
	quit;
	proc sort data=idn.split_spec; by gvkey DATE_act datadate; run;

	data idn.split_spec;
		set idn.split_spec;
		aer=ret_local-(a+b*mkt_ret);
		if ret_lag_dif>10 then aer=.;
	run;


	data idn.split_window;
		set idn.split_spec;
		if count>w_start and count<=w_end;
	run;


	data idn.split_window;
		set idn.split_window;
		window=count-w_start;
	run;

	data idn.split_window;
		set idn.split_window;
		window2=window-61;
	run;

	data idn.split_window;
		set idn.split_window;
		if not missing(aer);
	run;
	data idn.see;
		set idn.split_window;
		if aer>1;
	run;

	data idn.split_aer;
		set idn.split_window;
		keep aer window2 pos sd_aer;
		if aer>=0 then pos=1;
		else pos=0; 
	run;

	proc sort data=idn.split_aer; by window2; run;

	proc sql;
		create table idn.split_aer as select *, sum(pos) as pos_sum, mean(aer) as mean, count(aer) as num from idn.split_aer group by window2;
	quit;

	data idn.split_aer;
		set idn.split_aer;
		pos_aer=pos_sum/num;
	run;

	data idn.split_aer;
		set idn.split_aer;
		constant=1;
	run;

	proc reg data=idn.split_aer  outest=idn.split_aermean  TABLEOUT  noprint;
		by window2;
		model aer = constant/noint;
	run;
	data idn.split_aermean;
		set idn.split_aermean;
		if _TYPE_="T";
	run;


	proc sort data=idn.split_aer nodupkey; by window2; run;

	data idn.split_aer;
		set idn.split_aer;
		keep window2 mean pos_aer;
	run;

	data idn.split_aer;
	    set idn.split_aer;
	    retain cum_aer;
	    cum_aer+mean;
	run;

	proc sql;
		create table idn.split_aer_&sub. as select a.*, b.constant as t_stat from idn.split_aer as a left join idn.split_aermean as b on a.window2=b.window2;
	quit;

	proc delete data=idn.split_spec_date idn.split_reg idn.split_ab idn.split_window idn.split_aermean idn.split_sd idn.split_aer; run;
%mend;

%split_aer(idn.split,all)
proc print data=idn.split_aer_All;
run;
	
data idn.split_sp;
	set idn.split;
	if split;
run;

%split_aer(idn.split_sp,sp)

data idn.split_rev;
	set idn.split;
	if split=0;
run;

%split_aer(idn.split_rev,rev)



%macro split_aer2(data,sub);
	proc sql;
		create table idn.split_spec as select a.*, b.DATE_ann, b.DATE_act, b.split ,b.AnnounceDate
	from idn.daily_idn as a, &data. as b where a.gvkey_n=b.gvkey and b.begin_ann<=a.datadate<=b.end_ann;
	quit;

	proc sort data=idn.split_spec; by gvkey DATE_ann datadate; run;

/*	proc sort data=idn.split; by gvkey AnnounceDate; run;*/


	data idn.split_spec;
	  set idn.split_spec;
	  count + 1;
	  by gvkey AnnounceDate;
	  if first.AnnounceDate then count = 1;
	run;

	data idn.split_spec_date;
		set idn.split_spec;
		if DATE_ann=datadate;
	run;

	data idn.split_spec_date; 
		set idn.split_spec_date;
		a_start=count-250;
		a_end=count-120;
		w_start=count-61;
		w_end=count+60;
	run;

	data idn.split_spec_date; 
		set idn.split_spec_date;
		if a_end>=52;
		if a_start<0 then a_start=0;
	run; 

	proc sql;
		create table idn.split_spec as select a.*, b.a_start, b.a_end , b.w_start, b.w_end
	from idn.split_spec as a left join idn.split_spec_date as b on a.gvkey_n=b.gvkey_n and a.DATE_ann=b.DATE_ann;
	quit;

	proc sort data=idn.split_spec; by gvkey DATE_ann datadate; run;

	data idn.split_reg;
		set idn.split_spec;
		if count>a_start and count<=a_end;
	run;

	proc reg data=idn.split_reg  outest=idn.split_ab noprint;
		by gvkey_n DATE_ann;
		model ret_local = mkt_ret;
	run;

	data idn.split_ab;
		set idn.split_ab;
		if intercept^=0;

	run;

	proc sql;
		create table idn.split_spec as select a.*, b.intercept as a, b.mkt_ret as b
	from idn.split_spec as a left join idn.split_ab as b on a.gvkey_n=b.gvkey_n and a.DATE_ann=b.DATE_ann;
	quit;
	proc sort data=idn.split_spec; by gvkey DATE_ann datadate; run;

	data idn.split_spec;
		set idn.split_spec;
		aer=ret_local-(a+b*mkt_ret);
		if ret_lag_dif>10 then aer=.;
	run;


	data idn.split_window;
		set idn.split_spec;
		if count>w_start and count<=w_end;
	run;


	data idn.split_window;
		set idn.split_window;
		window=count-w_start;
	run;

	data idn.split_window;
		set idn.split_window;
		window2=window-61;
	run;

	data idn.split_window;
		set idn.split_window;
		if not missing(aer);
	run;
	data idn.see;
		set idn.split_window;
		if aer>1;
	run;

	data idn.split_aer;
		set idn.split_window;
		keep aer window2 pos sd_aer;
		if aer>=0 then pos=1;
		else pos=0; 
	run;

	proc sort data=idn.split_aer; by window2; run;

	proc sql;
		create table idn.split_aer as select *, sum(pos) as pos_sum, mean(aer) as mean, count(aer) as num from idn.split_aer group by window2;
	quit;

	data idn.split_aer;
		set idn.split_aer;
		pos_aer=pos_sum/num;
	run;

	data idn.split_aer;
		set idn.split_aer;
		constant=1;
	run;

	proc reg data=idn.split_aer  outest=idn.split_aermean  TABLEOUT  noprint;
		by window2;
		model aer = constant/noint;
	run;
	data idn.split_aermean;
		set idn.split_aermean;
		if _TYPE_="T";
	run;


	proc sort data=idn.split_aer nodupkey; by window2; run;

	data idn.split_aer;
		set idn.split_aer;
		keep window2 mean pos_aer;
	run;

	data idn.split_aer;
	    set idn.split_aer;
	    retain cum_aer;
	    cum_aer+mean;
	run;

	proc sql;
		create table idn.split_aer_&sub. as select a.*, b.constant as t_stat from idn.split_aer as a left join idn.split_aermean as b on a.window2=b.window2;
	quit;

	proc delete data=idn.split_spec_date idn.split_reg idn.split_ab idn.split_window idn.split_aermean idn.split_sd idn.split_aer; run;
%mend;

%split_aer2(idn.split_ann,all_ann)
proc print data=idn.split_aer_all_ann;
run;
data idn.split_ann_sp;
	set idn.split_ann;
	if split;
run;

%split_aer2(idn.split_ann_sp,sp_ann)

data idn.split_ann_rev;
	set idn.split_ann;
	if split=0;
run;

%split_aer2(idn.split_ann_rev,rev_ann)

%let list=split_aer_all split_aer_sp split_aer_rev split_aer_all_ann split_aer_sp_ann;
%let path=E:/IDN_Market/Results/;
%macro export();
	%let nwords=%sysfunc(countw(&list.));	
	%do i=1 %to &nwords;
		%let var=%scan(&list.,&i);
		%let way= "&path.&var..csv";

		data idn.&var.;
			set idn.&var.;
			if window2=-60 or window2=-40 or window2=-20 or window2>=-10;
			if window2=60 or window2=40 or window2=20 or window2<=10;
		run;
		PROC EXPORT DATA= idn.&var.
			OUTFILE= &way.
		    DBMS=csv REPLACE;     
		RUN; 
	%end;
%mend;
%export()

data idn.split_ann_sp;
	set idn.split_ann_sp;
	gap=intck('day', date_ann, date_act);
run;

proc sql;
	create table idn.split_gaps as select mean(gap) as mean, min(gap) as min, max(gap) as max, median(gap) as median from idn.split_ann_sp;
quit;



/*BH*/
data idn.split_spec_bh;
	set idn.split_spec;
	if count>w_start and count<=w_end;
	if not missing(aer);
run;


data idn.split_spec_bh;
   set idn.split_spec_bh;
   by gvkey DATE_act;
   retain cum_adjust;
   if first.DATE_act then cum_adjust = aer+1;
   else cum_adjust = cum_adjust*(aer+1) ;
run;

data idn.split_spec_bh;
   set idn.split_spec_bh;
   by gvkey DATE_act;
   cum_adjust=cum_adjust-1;
run;
/**/
/*data idn.split_spec_bh;*/
/*   set idn.split_spec_bh;*/
/*   by gvkey DATE_ann;*/
/*   retain cum_adjust;*/
/*   if first.DATE_ann then cum_adjust = aer+1;*/
/*   else cum_adjust = cum_adjust*(aer+1) ;*/
/*run;*/
/**/
/*data idn.split_spec_bh;*/
/*   set idn.split_spec_bh;*/
/*   by gvkey DATE_ann;*/
/*   cum_adjust=cum_adjust-1;*/
/*run;*/


	data idn.split_window;
		set idn.split_spec_bh;
		if count>w_start and count<=w_end;
	run;


	data idn.split_window;
		set idn.split_window;
		window=count-w_start;
	run;

	data idn.split_window;
		set idn.split_window;
		window2=window-61;
	run;


	data idn.split_aer;
		set idn.split_window;
		keep cum_adjust aer window2 pos sd_aer;
		if aer>=0 then pos=1;
		else pos=0; 
	run;

	proc sort data=idn.split_aer; by window2; run;

	proc sql;
		create table idn.split_aer as select *, sum(pos) as pos_sum, mean(cum_adjust) as mean, count(aer) as num from idn.split_aer group by window2;
	quit;

	data idn.split_aer;
		set idn.split_aer;
		pos_aer=pos_sum/num;
	run;

	data idn.split_aer;
		set idn.split_aer;
		constant=1;
	run;

	proc reg data=idn.split_aer  outest=idn.split_aermean  TABLEOUT  noprint;
		by window2;
		model  cum_adjust= constant/noint;
	run;
	data idn.split_aermean;
		set idn.split_aermean;
		if _TYPE_="T";
	run;


	proc sort data=idn.split_aer nodupkey; by window2; run;

	data idn.split_aer;
		set idn.split_aer;
		keep window2 mean pos_aer;
	run;

	proc sql;
		create table idn.split_aer as select a.*, b.constant as t_stat from idn.split_aer as a left join idn.split_aermean as b on a.window2=b.window2;
	quit;

	data idn.split_aer;
			set idn.split_aer;
			if window2=-60 or window2=-40 or window2=-20 or window2>=-10;
			if window2=60 or window2=40 or window2=20 or window2<=10;
		run;

	proc print data=idn.split_aer;
	run;

proc sgplot data=idn.split_aer;
    series x=window2 y=mean;
run;

proc delete data=idn.split_aer idn.split_aermean idn.split_window; run;

dm 'odsresults; clear';



data idn.split_spec_bh;
	set idn.split_spec;
	if count>w_start and count<=w_end;
	if not missing(aer);
	window=count-w_start;
	window2=window-61;
run;


data idn.split_spec_bh;
	set idn.split_spec_bh;
	if window2>=-10;

run;

data idn.split_spec_bh;
   set idn.split_spec_bh;
   by gvkey DATE_act;
   retain cum_adjust;
   if first.DATE_act then cum_adjust = aer+1;
   else cum_adjust = cum_adjust*(aer+1) ;
run;

data idn.split_spec_bh;
   set idn.split_spec_bh;
   by gvkey DATE_act;
   cum_adjust=cum_adjust-1;
run;

proc sql;
	create table idn.split_spec_bh as select a.*, b.sharia_stock from idn.split_spec_bh as a left join idn.cross_hold as b on a.gvkey_n=b.gvkey;
quit;

proc sort data=idn.split_spec_bh; by gvkey date_ann mdate; run;

/*data idn.split_spec_bh;*/
/*   set idn.split_spec_bh;*/
/*   by gvkey DATE_ann;*/
/*   retain cum_adjust;*/
/*   if first.DATE_ann then cum_adjust = aer+1;*/
/*   else cum_adjust = cum_adjust*(aer+1) ;*/
/*run;*/
/**/
/*data idn.split_spec_bh;*/
/*   set idn.split_spec_bh;*/
/*   by gvkey DATE_ann;*/
/*   cum_adjust=cum_adjust-1;*/
/*run;*/



	data idn.split_aer;
		set idn.split_spec_bh;
		keep cum_adjust aer window2 pos sd_aer;
		if aer>=0 then pos=1;
		else pos=0; 
		if sharia_stock=1;
	run;

	proc sort data=idn.split_aer; by window2; run;

	proc sql;
		create table idn.split_aer as select *, sum(pos) as pos_sum, mean(cum_adjust) as mean, count(aer) as num from idn.split_aer group by window2;
	quit;

	data idn.split_aer;
		set idn.split_aer;
		pos_aer=pos_sum/num;
	run;

	data idn.split_aer;
		set idn.split_aer;
		constant=1;
	run;

	proc reg data=idn.split_aer  outest=idn.split_aermean  TABLEOUT  noprint;
		by window2;
		model  cum_adjust= constant/noint;
	run;
	data idn.split_aermean;
		set idn.split_aermean;
		if _TYPE_="T";
	run;


	proc sort data=idn.split_aer nodupkey; by window2; run;

	data idn.split_aer;
		set idn.split_aer;
		keep window2 mean pos_aer;
	run;

	proc sql;
		create table idn.split_aer as select a.*, b.constant as t_stat from idn.split_aer as a left join idn.split_aermean as b on a.window2=b.window2;
	quit;
	proc sgplot data=idn.split_aer;
	    series x=window2 y=mean;
	run;
/*	data idn.split_aer;*/
/*			set idn.split_aer;*/
/*			if window2=-60 or window2=-40 or window2=-20 or window2>=-10;*/
/*			if window2=60 or window2=40 or window2=20 or window2<=10;*/
/*		run;*/

	proc print data=idn.split_aer;
	run;



proc delete data=idn.split_aer idn.split_aermean idn.split_window; run;

dm 'odsresults; clear';
