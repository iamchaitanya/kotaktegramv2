import sys
content = open('frontend/app.js').read()
original = content

# Step 2 — syncStrategyModalToState: add signal_trail sync
old2 = "    const slFixedInput = $('#sl-fixed-price');\n    if (slFixedInput && s.slFixed) slFixedInput.value = s.slFixed;\n\n    // [11] Sync compare mode toggle"
new2 = """    const slFixedInput = $('#sl-fixed-price');
    if (slFixedInput && s.slFixed) slFixedInput.value = s.slFixed;
    const slSignalTrailRow = $('#sl-signal-trail-row');
    if (slSignalTrailRow) slSignalTrailRow.style.display = s.trailingSL === 'signal_trail' ? 'block' : 'none';
    const actInput = $('#sl-activation-points');
    if (actInput) actInput.value = s.activationPoints ?? 5;
    const gapInput = $('#sl-trail-gap');
    if (gapInput) gapInput.value = s.trailGap ?? 2;

    // [11] Sync compare mode toggle"""
assert old2 in content, 'MATCH FAILED step 2'
content = content.replace(old2, new2, 1)
print('Step 2 OK')

# Step 4 — btnSave: read activationPoints and trailGap
old4 = "            const slFixedVal = trailingSL === 'fixed' ? (parseFloat($('#sl-fixed-price')?.value) || null) : null;\n\n            // [11] Read compare mode toggle"
new4 = """            const slFixedVal = trailingSL === 'fixed' ? (parseFloat($('#sl-fixed-price')?.value) || null) : null;
            const activationPoints = trailingSL === 'signal_trail' ? (parseFloat($('#sl-activation-points')?.value) || 5) : null;
            const trailGap = trailingSL === 'signal_trail' ? (parseFloat($('#sl-trail-gap')?.value) || 2) : null;

            // [11] Read compare mode toggle"""
assert old4 in content, 'MATCH FAILED step 4'
content = content.replace(old4, new4, 1)
print('Step 4 OK')

# Step 5 — btnSave: validate + include in state.strategy
old5 = "            if (trailingSL === 'fixed' && !slFixedVal) {\n                toast('Please enter a fixed SL price', 'warning');\n                return;\n            }\n\n            state.strategy = { lots, entryLogic, entryAvgPick, entryFixed: entryFixedVal, trailingSL, slFixed: slFixedVal, compareMode };"
new5 = """            if (trailingSL === 'fixed' && !slFixedVal) {
                toast('Please enter a fixed SL price', 'warning');
                return;
            }
            if (trailingSL === 'signal_trail' && (!activationPoints || !trailGap)) {
                toast('Please enter activation pts and trail gap for Signal Trail', 'warning');
                return;
            }

            state.strategy = { lots, entryLogic, entryAvgPick, entryFixed: entryFixedVal, trailingSL, slFixed: slFixedVal, activationPoints, trailGap, compareMode };"""
assert old5 in content, 'MATCH FAILED step 5'
content = content.replace(old5, new5, 1)
print('Step 5 OK')

open('frontend/app.js', 'w').write(content)
print('All done — app.js saved.')
