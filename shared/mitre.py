"""
Local MITRE ATT&CK mapping (Section 25, 51).

This is a small, curated subset of the ATT&CK Enterprise matrix — just the
techniques CyberStream's detection engine can actually produce evidence for.
It ships as a local dict so the pipeline never needs network access to
label a detection.
"""

MITRE_TECHNIQUES = {
    "brute_force": {"id": "T1110", "name": "Brute Force",
                     "tactic": "Credential Access"},
    "password_spraying": {"id": "T1110.003", "name": "Password Spraying",
                           "tactic": "Credential Access"},
    "valid_accounts": {"id": "T1078", "name": "Valid Accounts",
                        "tactic": "Defense Evasion / Persistence"},
    "port_scan": {"id": "T1046", "name": "Network Service Discovery",
                  "tactic": "Discovery"},
    "privilege_escalation": {"id": "T1068", "name": "Exploitation for Privilege Escalation",
                              "tactic": "Privilege Escalation"},
    "account_manipulation": {"id": "T1098", "name": "Account Manipulation",
                              "tactic": "Persistence"},
    "suspicious_process": {"id": "T1059", "name": "Command and Scripting Interpreter",
                            "tactic": "Execution"},
    "data_transfer_anomaly": {"id": "T1041", "name": "Exfiltration Over C2 Channel",
                               "tactic": "Exfiltration"},
    "dns_anomaly": {"id": "T1071.004", "name": "Application Layer Protocol: DNS",
                     "tactic": "Command and Control"},
    "suspicious_authentication": {"id": "T1078.002", "name": "Valid Accounts: Domain Accounts",
                                    "tactic": "Defense Evasion / Initial Access"},
}


def mitre_for(detection_rule: str) -> dict | None:
    return MITRE_TECHNIQUES.get(detection_rule)
