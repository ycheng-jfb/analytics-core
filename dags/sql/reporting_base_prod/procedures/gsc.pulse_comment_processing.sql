merge into reporting_base_prod.gsc.rpt_pipeline_detail tgt
using (
        select c.row_key,
            case
                when nvl(r.comments, 'mikewashere') = 'mikewashere'
                then c.comment
                when c.comment = r.comments
                then c.comment
                else c.comment || ',' || r.comments
            end as comments
        from lake_view.evolve01_ssrs_reports.pulse_comments c
        join reporting_base_prod.gsc.rpt_pipeline_detail r
        on c.row_key = r.row_key
    ) src
    on tgt.row_key = src.row_key

when matched
then update
set tgt.comments = src.comments;
