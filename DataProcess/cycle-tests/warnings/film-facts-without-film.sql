select FA.* from dw.film_fact FA
  left join dw.film_dim FD using (film_id)
 where FD.film_id is null
