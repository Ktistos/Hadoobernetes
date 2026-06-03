from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2] / "k8s_resources"


def _load_docs():
    docs = []
    for path in sorted(ROOT.rglob("*.yaml")):
        for doc in yaml.safe_load_all(path.read_text()):
            if doc:
                docs.append((path, doc))
    return docs


def _namespace(doc):
    return doc.get("metadata", {}).get("namespace", "default")


def _pod_specs(doc):
    kind = doc.get("kind")
    if kind in {"Deployment", "StatefulSet", "DaemonSet"}:
        yield doc["spec"]["template"]["spec"]
    elif kind == "Job":
        yield doc["spec"]["template"]["spec"]


def _containers(pod_spec):
    yield from pod_spec.get("initContainers", [])
    yield from pod_spec.get("containers", [])


def test_secret_key_refs_point_to_secrets_in_same_namespace():
    docs = _load_docs()
    secrets = {
        (_namespace(doc), doc["metadata"]["name"])
        for _, doc in docs
        if doc.get("kind") == "Secret"
    }

    missing = []
    for path, doc in docs:
        namespace = _namespace(doc)
        for pod_spec in _pod_specs(doc):
            for container in _containers(pod_spec):
                for env in container.get("env", []):
                    ref = env.get("valueFrom", {}).get("secretKeyRef")
                    if ref and (namespace, ref["name"]) not in secrets:
                        missing.append(f"{path}:{namespace}/{ref['name']}")

    assert missing == []


def test_cluster_manager_has_internal_update_token_secret_ref():
    docs = dict(
        (doc["metadata"]["name"], doc)
        for _, doc in _load_docs()
        if doc.get("kind") == "Deployment" and _namespace(doc) == "mapreduce"
    )
    deployment = docs["cluster-manager"]
    env = {
        item["name"]: item
        for item in deployment["spec"]["template"]["spec"]["containers"][0]["env"]
    }

    token_ref = env["INTERNAL_UPDATE_TOKEN"]["valueFrom"]["secretKeyRef"]
    assert token_ref == {"name": "internal-secret", "key": "INTERNAL_UPDATE_TOKEN"}
