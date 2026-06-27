-- Transit mode dimension.
select * from (
    values
        ('bus', 'Bus', 'CTA bus fleet (BusTime)'),
        ('train', 'Rail', 'CTA "L" rail lines (Train Tracker)')
) as t(mode, mode_name, description)
