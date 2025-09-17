CREATE OR REPLACE NETWORK RULE tableau_web_acc_rule
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = ('10ay.online.tableau.com');
