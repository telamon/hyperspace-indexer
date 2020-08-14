const sharp = require('sharp')
module.exports = async function analyzeImage ({
  drive,
  readRemoteFile,
  log,
  writeEntry,
  setPreview
}, file) {
  const { body } = await readRemoteFile(drive, file, 1024 * 1024 * 10, true) // keep all images < 256kB
  try {
    const thumb = await sharp(body)
      .resize(200)
      .webp({ quality: 65, reductionEffort: 6 })
      .toBuffer()
    await setPreview(thumb)
  } catch (err) {
    log('Thumbnail generation failed', err)
    throw err
  }
  // TODO: run image through classifier to obtain search terms
  // https://github.com/onnx/models#image_classification
}
