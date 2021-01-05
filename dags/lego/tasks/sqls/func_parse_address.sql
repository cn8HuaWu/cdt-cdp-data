CREATE OR REPLACE FUNCTION edw.func_parse_address(address character varying)
 RETURNS character varying
 LANGUAGE plpgsql
AS $function$
  declare provices varchar[] := ARRAY['河北','山西','辽宁','吉林','江苏','浙江','安徽','福建','江西','山东','河南','湖北','湖南','广东','海南','四川','贵州','云南','陕西','甘肃','青海','台湾','广西','西藏','宁夏','新疆'];
  declare provices_len3 varchar[] := ARRAY['黑龙江','内蒙古'];
  declare d_provices varchar[]  := ARRAY['北京','天津','上海','重庆','香港','澳门'];
  declare g_provices varchar[] := ARRAY['内蒙古自治区 ', '广西壮族自治区', '西藏自治区', '宁夏回族自治区', '新疆维吾尔自治区'];
  declare leadstr varchar(100);
  declare provices_city  varchar(100);
  declare x   varchar(100);  
  declare myaddress varchar(500);
begin
	
	myaddress := replace(address,' ','');
	if (myaddress ~ E'(内蒙古|广西壮族|西藏|宁夏|新疆).*'  ) then
		leadstr := substring( left( myaddress, POSITION( '区' in myaddress)) from 1); 
	
		foreach x  in ARRAY g_provices 
		loop
			if (x = leadstr) then
				
				provices_city := leadstr || ',' || substring( left( myaddress, POSITION( '市' in myaddress)) from POSITION( '区' in myaddress)+1);
				return provices_city;
			end if;
		end loop;
	end if;
	
	leadstr :=  left(myaddress, 2);
	foreach x  in ARRAY provices 
	loop
		if(leadstr = x ) then
			provices_city := leadstr;
			if( left( myaddress, POSITION( '市' in myaddress)) ~ E'.*省.*' ) then
				provices_city := provices_city  ||  ',' ||substring( left( myaddress, POSITION( '市' in myaddress)) from 4);
			else
				provices_city := provices_city  ||  ',' ||substring( left( myaddress, POSITION( '市' in myaddress)) from 3);
			end if;
			return provices_city;
		end if;
		
	end loop ;

	
	foreach x in ARRAY d_provices 
	lOOP
		if(leadstr = x ) then
			provices_city := leadstr;
			provices_city := provices_city  ||',' ||leadstr;
			return provices_city;
		end if;
	end loop ;
	

	leadstr :=  left(myaddress, 3);
	foreach x  in ARRAY provices_len3 
	lOOP
		if(leadstr = x ) then
			provices_city := leadstr;
			if( left( myaddress, POSITION( '市' in myaddress)) ~ E'.*省.*' ) then
				provices_city := provices_city  ||  ',' ||substring( left( myaddress, POSITION( '市' in myaddress)) from 5);
			else
				provices_city := provices_city  ||  ',' ||substring( left( myaddress, POSITION( '市' in myaddress)) from 4);
			end if;
			
			return provices_city;
		end if;
	end loop;

	return null;

end ;
$function$
;
