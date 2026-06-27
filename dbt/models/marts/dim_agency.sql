-- Conformed agency dimension. CTA today; Metra/Pace are additive later.
select * from (
    values
        ('CTA', 'Chicago Transit Authority', 'Chicago, IL', true)
) as t(agency, agency_name, locale, is_active)
