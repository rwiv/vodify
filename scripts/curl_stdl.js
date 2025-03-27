async function main() {
  const [endpoint, status, ptype, uid, vidname] = process.argv.slice(2);
  const res = await fetch(`${endpoint}/api/stdl/done`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      status, ptype, uid, vidname, fstype: "local",
    }),
  });
  console.log(await res.json());
}

main().catch(console.error);
