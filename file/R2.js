import util from "node:util";
import clientS3 from "@aws-sdk/client-s3";
import S3 from "./S3.js";
import { r2ClientConfig } from "./credentials.js";
const { S3Client, } = clientS3;
function R2(worker) {
    S3.call(this, worker);
    this.prefix = 'r2';
}
util.inherits(R2, S3);
R2.prototype.getClient = function () {
    if (!this.client) {
        if (this.resolvedCredentials) {
            this.client = new S3Client(r2ClientConfig(this.resolvedCredentials));
        } else {
            const missing = ['CLOUDFLARE_R2_ACCOUNT_ID', 'CLOUDFLARE_R2_ACCESS_KEY_ID', 'CLOUDFLARE_R2_SECRET_ACCESS_KEY']
                .filter((r) => !process.env[r]);
            if (missing.length > 0)
                throw new Error(`Missing environment variables for Cloudflare access:${missing.join(',')}`);
            const ACCOUNT_ID = process.env.CLOUDFLARE_R2_ACCOUNT_ID;
            const ACCESS_KEY_ID = process.env.CLOUDFLARE_R2_ACCESS_KEY_ID;
            const SECRET_ACCESS_KEY = process.env.CLOUDFLARE_R2_SECRET_ACCESS_KEY;
            this.client = new S3Client({
                // R2 does not strictly require a region, but the SDK expects one. 'auto' works fine.
                region: 'auto',
                endpoint: `https://${ACCOUNT_ID}.r2.cloudflarestorage.com`,
                credentials: {
                    accessKeyId: ACCESS_KEY_ID,
                    secretAccessKey: SECRET_ACCESS_KEY,
                },
                forcePathStyle: true, // Important for R2 compatibility
            });
        }
    }
    return this.client;
};
export default R2;
