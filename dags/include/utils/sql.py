from include.utils.string import unindent_auto


def clean_col_list(col_list):
    """
    Converts column list to match database naming conventions

    e.g. 'Some Col (dma)' becomes some_col_dma

    :param col_list:
    :return: converted column list
    """
    trans_del = str.maketrans(dict.fromkeys("()[]{}"))
    trans_repl = str.maketrans("- /", "___")
    clean_list = list(map(lambda x: x.lower().translate(trans_repl).translate(trans_del), col_list))
    return clean_list


def build_bcp_in_command(
    database: str,
    schema: str,
    table: str,
    path: str,
    server: str,
    login: str,
    password: str,
    record_delimiter: str,
    field_delimiter: str,
) -> str:
    cmd = r"""
        bcp {database}.{schema}.{table} \
            in '{path}' \
            -S '{server}' \
            -U '{login}' \
            -P '{password}' \
            -c \
            -t {field_delimiter} \
            -r {record_delimiter}""".format(
        database=database,
        schema=schema,
        table=table,
        path=path,
        login=login,
        server=server,
        record_delimiter=record_delimiter,
        field_delimiter=field_delimiter,
        password=password,
    )
    return unindent_auto(cmd)


def build_bcp_command(query, queryout, server, login, password, record_delimiter, field_delimiter):
    cmd = r"""
        bcp "{query}" \
            queryout {queryout}  \
            -S {server} \
            -U {login} \
            -P {password} \
            -a 65535 \
            -c \
            -r {record_delimiter} \
            -t {field_delimiter}""".format(
        login=login,
        server=server,
        query=query,
        queryout=queryout,
        record_delimiter=record_delimiter,
        field_delimiter=field_delimiter,
        password=password,
    )

    return unindent_auto(cmd)


def build_bcp_to_s3_bash_command(
    login, server, query, bucket, key, record_delimiter, field_delimiter, password='"$BCP_PASS"'
):
    bash_command_template = r"""
    #!/bin/bash
    tfifo=$(mktemp -d)/fifo
    mkfifo $tfifo
    {{bcp_command}} &
    pid=$! # get PID of backgrounded bcp process
    count=$(ps -p $pid -o pid= |wc -l) # check whether process is still running
    if [[ $count -eq 0 ]] # if process is already terminated, something went wrong
    then
        echo "something went wrong with bcp command"
        rm $tfifo
        wait $pid
        exit $?
    else
        echo "bcp command still running"
        cat $tfifo | gzip | aws s3 cp - s3://{bucket}/{key} && rm $tfifo
        exit $?
    fi
    """.format(  # type: ignore
        bucket=bucket,
        key=key,
    )
    bash_command_template = unindent_auto(bash_command_template)

    bcp_command = build_bcp_command(
        login=login,
        server=server,
        query=query,
        queryout="$tfifo",
        record_delimiter=record_delimiter,
        field_delimiter=field_delimiter,
        password=password,
    )

    bash_command = bash_command_template.format(bcp_command=bcp_command)
    return bash_command
