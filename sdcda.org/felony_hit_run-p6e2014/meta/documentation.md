**{about_summary}**

This is a conversion of felony hit and run data provided by the San Diego district attorney. The file retains the original columns, with a few additions. The most important of which is that the `Sentencing` column is broken out into seperate components. Here are the column names and the original descriptino from the `Sentencing` column. 

* `incar`	Any Incarceration
* `incar_county`	Prison Served in County Jail
* `incar_life`	Life and Life +
* `incar_prison`	State Prison
* `incar_local`	Local Custody
* `prob`	Any probation
* `prob_formal`	Probation Formal
* `prob_sum`	Probation Summary
* `prog`	Any program
* `prog_child`	Child Abuse Program
* `prog_dv`	DV Recovery Program
* `work`	Any work
* `work_furl`	Work Furlough
* `work_pub`	Public Work Service
* `work_vol`	Volunteer Work Service

The values in the columns are in days, converted from the original "Years" and "Months" ( 30.4 days per month. )

The Names are heirarchical. For instance, when there is a value of `10` in the  `prob_formal`, the same value will appear in the `prob` column, allowing you to analyze either the specific types of probations, or the general calss of all probation types.  

Additionally, the modifiers in the `highest_charge` field are broken out into `highest_charge_mod`
