drop table if exists public.shipping_country_rates cascade;
drop sequence if exists public.d_shipping_country_rates_sequence;
create sequence if not exists public.d_shipping_country_rates_sequence START 1;
create table if not exists public.shipping_country_rates
(
    id bigint default nextval('d_shipping_country_rates_sequence'),
    shipping_country           text,
    shipping_country_base_rate numeric(14, 3),
    primary key (id)
);
insert into public.shipping_country_rates (
    shipping_country,
    shipping_country_base_rate)
select
    shipping_country,
    shipping_country_base_rate
from public.shipping
group by 1,2;
drop table if exists public.shipping_agreement cascade;
drop sequence if exists public.d_shipping_agreement_sequence;
create sequence if not exists public.d_shipping_agreement_sequence START 1;
create table if not exists public.shipping_agreement
(
    id bigint default nextval('d_shipping_agreement_sequence'),
    agreement_id         integer,
    agreement_number     text,
    agreement_rate       numeric(4, 2),
    agreement_commission numeric(4, 2),
    primary key (id)
);
insert into public.shipping_agreement (
    agreement_id,
    agreement_number,
    agreement_rate,
    agreement_commission)
select
    (regexp_split_to_array(vendor_agreement_description,':'))[1]::int as agreement_id,
    (regexp_split_to_array(vendor_agreement_description,':'))[2] as agreement_number,
    (regexp_split_to_array(vendor_agreement_description,':'))[3]::numeric(4,2) as agreement_rate,
    (regexp_split_to_array(vendor_agreement_description,':'))[4]::numeric(4,2) as agreement_commission
from public.shipping
group by 1,2,3,4;
;
drop table if exists public.shipping_transfer cascade;
drop sequence if exists public.d_shipping_transfer_sequence;
create sequence if not exists public.d_shipping_transfer_sequence START 1;
create table if not exists public.shipping_transfer
(
    id bigint default nextval('d_shipping_transfer_sequence'),
    transfer_type          text,
    transfer_model         text,
    shipping_transfer_rate numeric(14, 3),
    primary key (id)
);
insert into public.shipping_transfer (
    transfer_type,
    transfer_model,
    shipping_transfer_rate)
select
    (regexp_split_to_array(shipping_transfer_description,':'))[1] as transfer_type,
    (regexp_split_to_array(shipping_transfer_description,':'))[2] as transfer_model,
    max(shipping_transfer_rate) as shipping_transfer_rate
from public.shipping
group by 1,2;
drop table if exists public.shipping_info;
create table if not exists public.shipping_info
(
    shipping_id                 bigint,
    vendor_id                   bigint,
    payment_amount              numeric(14, 2),
    shipping_plan_datetime      timestamp,
    shipping_transfer_id        bigint,
    shipping_agreement_id       bigint,
    shipping_country_rate_id    bigint,
    primary key (shipping_id),
    foreign key (shipping_transfer_id) references shipping_transfer (id),
    foreign key (shipping_agreement_id) references shipping_agreement (id),
    foreign key (shipping_country_rate_id) references shipping_country_rates (id)
);
insert into public.shipping_info (
    shipping_id,
    vendor_id,
    payment_amount,
    shipping_plan_datetime,
    shipping_transfer_id,
    shipping_agreement_id,
    shipping_country_rate_id)
select
    distinct on (sh.shippingid) sh.shippingid as shipping_id,
    sh.vendorid as vendor_id,
    sh.payment_amount,
    sh.shipping_plan_datetime,
    sht.id as shipping_transfer_id,
    sha.id as shipping_agreement_id,
    shc.id as shipping_country_rate_id
from public.shipping as sh
left join public.shipping_transfer as sht on sh.shipping_transfer_description = sht.transfer_type||':'||sht.transfer_model
left join public.shipping_agreement as sha
    on sh.vendor_agreement_description = agreement_id||':'||agreement_number||':'||agreement_rate||':'||agreement_commission
left join public.shipping_country_rates as shc on sh.shipping_country = shc.shipping_country;
;
drop table if exists public.shipping_status;
create table if not exists public.shipping_status
(
    shipping_id                  bigint,
    status                       text,
    state                        text,
    shipping_start_fact_datetime timestamp,
    shipping_end_fact_datetime   timestamp,
    primary key (shipping_id)
);
insert into public.shipping_status (
    shipping_id,
    status,
    state,
    shipping_start_fact_datetime,
    shipping_end_fact_datetime)
select
    distinct on (shippingid) shippingid as shipping_id,
    status,
    state,
    max(case when state = 'booked' then state_datetime end) over (partition by shippingid) shipping_start_fact_datetime,
    max(case when state = 'recieved' then state_datetime end) over (partition by shippingid) shipping_end_fact_datetime
from public.shipping
order by shippingid, state_datetime desc
;
create or replace view public.shipping_datamart as
select
    si.shipping_id,
    si.vendor_id,
    st.transfer_type,
    date_part('day', ss.shipping_end_fact_datetime - ss.shipping_start_fact_datetime) as full_day_at_shipping,
    case when shipping_end_fact_datetime > shipping_plan_datetime then 1 else 0 end as is_delay,
    case when status = 'finished' then 1 else 0 end as is_shipping_finish,
    greatest(0,date_part('day', shipping_end_fact_datetime - shipping_plan_datetime)) as delay_day_at_shipping,
--     payment_amount,
    payment_amount*(shipping_country_base_rate+agreement_rate+shipping_transfer_rate) as vat,
    payment_amount*agreement_commission as profit
from public.shipping_info as si
left join public.shipping_transfer st on st.id = si.shipping_transfer_id
left join public.shipping_agreement sa on sa.id = si.shipping_agreement_id
left join public.shipping_country_rates scr on si.shipping_country_rate_id = scr.id
left join public.shipping_status ss on si.shipping_id = ss.shipping_id;
