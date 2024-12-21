select S.* from dw.store_dim S
  left join dw.inventory_fact I using (store_id)
 where I.store_id is null
   and S.last_update + interval '3 days' < now()
