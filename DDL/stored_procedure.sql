create procedure updatewatermarktabke
 @lastload varchar(200)
as
begin
  begin transaction;
  update [dbo].[water_table]
  set last_load = @lastload
	commit transaction;
end ;
