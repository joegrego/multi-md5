# multi-md5
Uses python threads to create or verify md5 checksum files


At the University of Michigan's Advanced Genomics Core, we calculate and check md5 checksums. A lot. And just doing something like

```
find . -type f -not -name "*.md5" -not -path '*/\.*' -print0 | xargs -0 md5sum | tee -a the.md5
```

works, but it is single threaded, one at a time, and can take many hours.  So, we thought, why not make it multithreaded? Then we thought, well, how would we do that. So I asked the UMGPT, and it spit out some great starter code, and I took that and ran with it.

For me, this is super useful, because I can very quickly multhreadedly calculate and check md5s:
```
python3 multi-md5.py . --out the.md5
python3 multi-md5.py . --verify the.md5
```

If you want to include hidden ("dot") files and dirs when you create the md5, you can. If you don't like the "./" prefix (we're used to it), you can turn it off. Verbose mode is moderately useful. Debug mode is way too chatty. 

When you verify, you can tell it to keep going if it hits a checksum that doesn't compare properly (or a missing file). It will still die horribly at the end, but that's what I want it to do.

It batches up the things it wants to launch in to threads so it doesn't overwhelm your box or take too long to get started. You can specify the read chunk size (it makes a good guess based on your filesystem if you leave it alone). You can specify more or less simultaneous worker threads, but I found that 5 worked well on both my mac and on our supercomputer cluster. If you play with anything here, I'd suggest trying `-read-size 32767` to see if if flies faster (remember, you're probably fighting with IO limits, not CPU/thread limits).


"I received assistance from an AI developed by OpenAI, utilized through the University of Michigan."
